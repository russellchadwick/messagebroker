package messagebroker

import (
	"fmt"
	"time"

	"github.com/gogits/gogs/modules/uuid"
	"github.com/jackc/pgx"
)

type PostgresqlMessageBroker struct {
	connPool *pgx.ConnPool
}

type PostgresqlConnectionConfig struct {
	Host     string // host (e.g. localhost) or path to unix domain socket directory (e.g. /private/tmp)
	Port     uint16 // default: 5432
	Database string
	User     string // default: OS user name
	Password string
}

func (e *PostgresqlMessageBroker) Publish(routing string, body []byte) error {

	id := newUniqueID()

	exchange := ""

	_, err := e.connPool.Exec(`INSERT INTO queue 
		(id, exchange, routing, created_on, body) 
		VALUES 
		($1, $2, $3, $4, $5)
		RETURNING pg_notify('new_queue', id)`,
		id, exchange, routing, time.Now(), body)
	if err != nil {
		return err
	}

	return nil
}

func (e *PostgresqlMessageBroker) Consume(queue string, onMessage func(body []byte)) error {

	conn, err := e.connPool.Acquire()
	if err != nil {
		return err
	}

	err = conn.Listen("new_queue")
	if err != nil {
		return err
	}
	for {
		notification, err := conn.WaitForNotification(time.Minute)
		if err == pgx.ErrNotificationTimeout {
			fmt.Println("Timeout waiting for notification")
			continue
		}
		if err != nil {
			return err
		}
		fmt.Println("PID:", notification.Pid, "Channel:", notification.Channel, "Payload:", notification.Payload)

		row := conn.QueryRow("SELECT exchange, routing, created_on, body FROM queue WHERE id=$1", notification.Payload)
		var exchange string
		var routing string
		var createdOn time.Time
		var body []byte
		err = row.Scan(&exchange, &routing, &createdOn, &body)
		if err != nil {
			panic(err)
		}

		onMessage(body)
	}

}

func NewPostgresqlMessageBroker(config *PostgresqlConnectionConfig) (*PostgresqlMessageBroker, error) {
	connPoolConfig := pgx.ConnPoolConfig{
		ConnConfig: pgx.ConnConfig{
			Host:     config.Host,
			Port:     config.Port,
			User:     config.User,
			Password: config.Password,
			Database: config.Database,
		},
		MaxConnections: 5,
		AfterConnect:   afterConnect,
	}
	pool, err := pgx.NewConnPool(connPoolConfig)
	if err != nil {
		return nil, err
	}

	return &PostgresqlMessageBroker{
		connPool: pool,
	}, nil
}

func afterConnect(conn *pgx.Conn) error {
	_, err := conn.Exec(`CREATE TABLE IF NOT EXISTS queue (
		id TEXT PRIMARY KEY, 
		exchange TEXT NOT NULL,
		routing TEXT NOT NULL,
		created_on TIMESTAMP WITH TIME ZONE NOT NULL, 
		body BYTEA NOT NULL
		);`)
	if err != nil {
		return err
	}

	return nil
}

func newUniqueID() string {
	uuid := uuid.NewV4()
	return uuid.String()
}
