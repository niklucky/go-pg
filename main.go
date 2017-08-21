package pg

import (
	"encoding/json"
	"errors"
	"time"
	"strings"
	"strconv"
	"fmt"
	"database/sql"
	_ "github.com/lib/pq" // PostgreSQL driver
	"github.com/lib/pq"
)

const driverName = "postgres"

const LOG = "log"
const ERROR = "error"

type DBConfig struct {
	User,
	Password,
	Host,
	Port,
	Database,
	SSLmode string
}

type Mapper struct {
	DBConfig          DBConfig
	Conn              *sql.DB
	Listener          *pq.Listener
	Source            string
	ConnectionInfo    string
	ListenIdleTimeout time.Duration
	Handler           func(interface{})
	Logger func(...interface{}) error
}

func (mapper *Mapper) Connect() error {
	dbConfig := mapper.DBConfig
	mapper.ConnectionInfo = fmt.Sprintf("postgres://%v:%v@%v:%v/%v?sslmode=%v",
		dbConfig.User,
		dbConfig.Password,
		dbConfig.Host,
		dbConfig.Port,
		dbConfig.Database,
		dbConfig.SSLmode,
	)
	conn, err := sql.Open(driverName, mapper.ConnectionInfo)
	if err != nil {
		fmt.Println("Connection error: ", err)
		return err
	}
	if conn == nil {
		return mapper.Log(ERROR, "Connection to PostgreSQL is nil", nil, nil)
	}
	mapper.Conn = conn
	return nil
}

func (pgm *Mapper) Load(source string, fields string, query interface{}) (*sql.Rows, error) {
	if pgm.Conn == nil {
		pgm.Connect()
	}

	SQL := "SELECT " + fields + " FROM " + source
	if query != nil {
		SQL += " WHERE " + query.(string)
	}
	SQL += ";"
	// fmt.Println(SQL)
	rows, err := pgm.Conn.Query(SQL)
	if err != nil {
		return rows, err
	}
	return rows, nil
}

func (m *Mapper) InsertBatch(fields []string, rows []interface{}, onDuplicate interface{}) error {
	if len(rows) == 0 {
		return nil
	}
	if m.Conn == nil {
		m.Connect()
	}
	var values = []interface{}{}
	SQL := "insert into " + m.Source + " (" + strings.Join(fields, ",") + ") values "

	var placeholder []string

	counter := 0
	for _, row := range rows {
		r := row.([]interface{})
		var pl []string
		for i := 0; i < len(r); i++ {
			counter++
			pl = append(pl, "$" + strconv.Itoa(counter))
			values = append(values, r[i])
		}
		placeholder = append(placeholder, "("+strings.Join(pl, ",")+")")
	}
	SQL += strings.Join(placeholder, ",")
	// SQL = SQL[0 : len(SQL)-1]
	if onDuplicate != nil {
		SQL += " ON CONFLICT " + onDuplicate.(string)
	}
	stmt, err := m.Conn.Prepare(SQL)
	if err != nil {
		fmt.Println("stmt: ", SQL)
		return err
	}
	defer stmt.Close()
	_, execErr := stmt.Exec(values...)
	if execErr != nil {
		fmt.Println("Exec: ", execErr)
		return execErr
	}
	return nil
}
func (m *Mapper) Insert(fields []string, rows interface{}, onDuplicate interface{}) error {
	var data []interface{}
	data = append(data, rows)
	return m.InsertBatch(fields, data, onDuplicate)
}

func (mapper *Mapper) Query(query string) (*sql.Rows, error) {
	if mapper.Conn == nil {
		err := mapper.Connect()
		if err != nil {
			return nil, err
		}
	}
	return mapper.Conn.Query(query)
}

func (mapper *Mapper) Listen() error {
	if mapper.Conn == nil {
		mapper.Connect()
	}
	mapper.Log(LOG, "Listen " + mapper.DBConfig.Host + "/" + mapper.DBConfig.Database + " connecting")
	reportProblem := func(ev pq.ListenerEventType, err error) {
		if err != nil {
			mapper.Log("Error", "pg_listener_create_error", err, nil)
		}
	}

	mapper.Listener = pq.NewListener(mapper.ConnectionInfo, 10*time.Second, time.Minute, reportProblem)
	err := mapper.Listener.Listen("finery")
	if err != nil {
		panic(err)
	}
	for {
		mapper.HandleListen()
	}
}

func (mapper *Mapper) HandleListen() {
	l := mapper.Listener
	for {
		select {
		case n := <-l.Notify:

			var data interface{}
			if n == nil {
				mapper.Log(ERROR, "Listener extra is nil: ", n.Extra)
				return
			}
			err := json.Unmarshal([]byte(n.Extra), &data)
			if err != nil {
				mapper.Log(ERROR, "Error processing JSON: ", err, nil)
				return
			}
			mapper.Handler(data)
			return
		case <-time.After(mapper.ListenIdleTimeout):
			timeout := mapper.ListenIdleTimeout.String()
			mapper.Log(LOG, mapper.GetDBInfo() + ": Received no events for " + timeout + ", checking connection")
			go func() {
				l.Ping()
			}()
			return
		}
	}
}

func (mapper *Mapper) SetHandler(handler func(interface{})) {
	mapper.Handler = handler
}

func (m *Mapper) GetDBInfo() string {
	return m.DBConfig.Host + "/" + m.DBConfig.Database
}

func (mapper *Mapper) Close() error {
	if mapper.Conn != nil {
		mapper.Log("log", mapper.GetDBInfo() + " closing connection")
		return mapper.Conn.Close()
	}
	return nil
}

func (mapper *Mapper) Log(data ...interface{}) error {
	fmt.Println(data)
	return errors.New(data[0].(string))
}
