package pg

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/lib/pq"
	_ "github.com/lib/pq" // PostgreSQL driver
)

const driverName = "postgres"

/*
LOG - logging type
*/
const LOG = "log"

/*
ERROR - logging type
*/
const ERROR = "error"

/*
DBConfig - Postgres config
*/
type DBConfig struct {
	User,
	Password,
	Host,
	Port,
	Database,
	SSLmode string
}

/*
Mapper - Postgres Mapper to simplify interaction with DB
*/
type Mapper struct {
	DBConfig          DBConfig
	Conn              *sql.DB
	Listener          *pq.Listener
	Source            string
	ConnectionInfo    string
	ListenIdleTimeout time.Duration
	Handler           func(interface{})
	Logger            func(...interface{}) error
}

/*
connect - connecting to DB
*/
func (pgm *Mapper) connect() error {
	dbConfig := pgm.DBConfig
	pgm.ConnectionInfo = fmt.Sprintf("postgres://%v:%v@%v:%v/%v?sslmode=%v",
		dbConfig.User,
		dbConfig.Password,
		dbConfig.Host,
		dbConfig.Port,
		dbConfig.Database,
		dbConfig.SSLmode,
	)
	conn, err := sql.Open(driverName, pgm.ConnectionInfo)
	if err != nil {
		fmt.Println("Connection error: ", err)
		return err
	}
	if conn == nil {
		return pgm.Log(ERROR, "Connection to PostgreSQL is nil", nil, nil)
	}
	pgm.Conn = conn
	return nil
}

/*
Load - selecting data from DB
*/
func (pgm *Mapper) Load(source string, fields string, query interface{}) (*sql.Rows, error) {
	if err := pgm.checkConnection(); err != nil {
		return nil, err
	}

	SQL := "SELECT " + fields + " FROM " + source
	if query != nil {
		SQL += " WHERE " + query.(string)
	}
	SQL += ";"
	// fmt.Println(SQL)
	rows, err := pgm.Exec(SQL)
	if err != nil {
		return rows, err
	}
	return rows, nil
}

/*
Save — method inserts in DB row on duplicate key updates fields
*/
func (pgm *Mapper) Save(fields []string, values []interface{}, key map[string]interface{}) error {
	SQL := pgm.generateInsertQuery(fields)
	SQL += pgm.generateOnConflictQuery(fields, key)
	return pgm.execute(SQL, values)
}

/*
Create - creating new row in DB. Does not updates on conflict
*/
func (pgm *Mapper) Create(fields []string, values []interface{}) error {
	SQL := pgm.generateInsertQuery(fields)
	return pgm.execute(SQL, values)
}

func (pgm *Mapper) execute(SQL string, values []interface{}) error {
	if err := pgm.checkConnection(); err != nil {
		return err
	}

	stmt, err := pgm.Conn.Prepare(SQL)
	if err != nil {
		fmt.Println("Preparing statement error: ", err, SQL)
		return err
	}
	defer stmt.Close()
	_, execErr := stmt.Exec(values...)
	if execErr != nil {
		fmt.Println("Exec error: ", execErr)
		return execErr
	}
	return nil
}

/*
Exec - executing prepared SQL string
*/
func (pgm *Mapper) Exec(SQL string) (*sql.Rows, error) {
	if err := pgm.checkConnection(); err != nil {
		return nil, err
	}
	return pgm.Conn.Query(SQL)
}

func (pgm *Mapper) checkConnection() error {
	if pgm.Conn == nil {
		return pgm.connect()
	}
	return nil
}
func (pgm *Mapper) generateInsertQuery(fields []string) string {
	SQL := "INSERT INTO " + pgm.Source + " (" + strings.Join(fields, ",") + ") VALUES "
	var placeholder []string

	for i := range fields {
		key := strconv.Itoa((i + 1))
		placeholder = append(placeholder, "$"+key)
	}
	SQL += "(" + strings.Join(placeholder, ",") + ")"
	return SQL
}
func (pgm *Mapper) generateOnConflictQuery(fields []string, keys map[string]interface{}) string {
	if len(keys) == 0 {
		return " ON CONFLICT DO NOTHING "
	}
	var idx []string
	for key := range keys {
		idx = append(idx, key)
	}
	SQL := " ON CONFLICT (" + strings.Join(idx, ",") + ") DO UPDATE SET "
	var placeholder []string
	for i, field := range fields {
		key := strconv.Itoa((i + 1))
		value := field + " = $" + key + " "
		placeholder = append(placeholder, value)
	}
	SQL += strings.Join(placeholder, ",")
	return SQL
}

func (pgm *Mapper) InsertBatch(fields []string, rows []interface{}, onDuplicate interface{}) error {
	if len(rows) == 0 {
		return nil
	}
	if err := pgm.checkConnection(); err != nil {
		return err
	}
	var values = []interface{}{}
	SQL := "insert into " + pgm.Source + " (" + strings.Join(fields, ",") + ") values "

	var placeholder []string

	counter := 0
	for _, row := range rows {
		r := row.([]interface{})
		var pl []string
		for i := 0; i < len(r); i++ {
			counter++
			pl = append(pl, "$"+strconv.Itoa(counter))
			values = append(values, r[i])
		}
		placeholder = append(placeholder, "("+strings.Join(pl, ",")+")")
	}
	SQL += strings.Join(placeholder, ",")
	// SQL = SQL[0 : len(SQL)-1]
	if onDuplicate != nil {
		SQL += " ON CONFLICT " + onDuplicate.(string)
	}
	stmt, err := pgm.Conn.Prepare(SQL)
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

func (pgm *Mapper) Listen() error {
	if err := pgm.checkConnection(); err != nil {
		return err
	}
	pgm.Log(LOG, "Listen "+pgm.DBConfig.Host+"/"+pgm.DBConfig.Database+" connecting")
	reportProblem := func(ev pq.ListenerEventType, err error) {
		if err != nil {
			pgm.Log("Error", "pg_listener_create_error", err, nil)
		}
	}

	pgm.Listener = pq.NewListener(pgm.ConnectionInfo, 10*time.Second, time.Minute, reportProblem)
	err := pgm.Listener.Listen("finery")
	if err != nil {
		panic(err)
	}
	for {
		pgm.HandleListen()
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
			mapper.Log(LOG, mapper.GetDBInfo()+": Received no events for "+timeout+", checking connection")
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
		mapper.Log("log", mapper.GetDBInfo()+" closing connection")
		return mapper.Conn.Close()
	}
	return nil
}

func (mapper *Mapper) Log(data ...interface{}) error {
	fmt.Println(data)
	return errors.New(data[0].(string))
}
