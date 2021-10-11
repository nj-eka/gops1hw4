package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"math/big"
	"math/rand"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgconn"
	"github.com/jackc/pgconn/stmtcache"
	"github.com/jackc/pgtype"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
	_ "github.com/jackc/pgx/v4/stdlib"
)

const (
	PG_HOST     = "127.0.0.1"
	PG_PORT     = "5432"
	PG_USER     = "gopher"
	PG_PASSWORD = "P@ssw0rd"
	PG_DB_NAME  = "gopher_corp"
)

func main() {
	rand.Seed(time.Now().UnixNano())
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	config, err := getConfig()
	if err != nil {
		return err
	}
	switch config.Mode {
	case 1:
		return runSQLInterfaceExample()
	case 2:
		return runPGXExample()
	case 3:
		return runPoolExample()
	case 4:
		return generateDBLoad(config)
	case 5:
		return runUpdateWithTX()
	case 6:
		return generateDBLoadWithPgBouncer(config)
	default:
		return fmt.Errorf("got an unexpected running mode %d", config.Mode)
	}
}

type EmpSalary struct {
	Name     string
	LastName string
	Salary   big.Float
}

const sqlGetTwentySalaries = `
SELECT first_name, last_name, salary::numeric
FROM employees
WHERE first_name = $1
LIMIT 20`

func runSQLInterfaceExample() error {
	connStr := ComposeConnectionString()
	db, err := sql.Open("pgx", connStr)
	if err != nil {
		return fmt.Errorf("failed to open a connection to the Database using %s: %w", connStr, err)
	}
	defer db.Close()
	rows, err := db.Query(sqlGetTwentySalaries, "Alice")
	if err != nil {
		return fmt.Errorf("failed to fetch data from the employees table: %w", err)
	}
	defer rows.Close()
	salaries := make([]EmpSalary, 0, 20)
	for rows.Next() {
		var empSalary EmpSalary
		var salaryStr string
		if err := rows.Scan(&empSalary.Name, &empSalary.LastName, &salaryStr); err != nil {
			return fmt.Errorf("failed to scan a returned row: %w", err)
		}
		salary, _, err := big.ParseFloat(salaryStr, 10, 2, big.ToNearestEven)
		if err != nil {
			return fmt.Errorf("failed to parse the salary value %s: %w", salaryStr, err)
		}
		empSalary.Salary = *salary
		salaries = append(salaries, empSalary)
	}
	for _, s := range salaries {
		log.Printf("%s %s: %s", s.Name, s.LastName, s.Salary.String())
	}
	return nil
}

type Config struct {
	Mode            int
	MaxConns        int32
	MinConns        int32
	AttackMS        time.Duration
	GoroutinesCount int
}

func getConfig() (*Config, error) {
	var mode, maxConns, minConns, goroutinesCount int
	var attackMS int64
	flag.IntVar(&mode, "mode", -1,
		`running mode of the application:
			- 1: with database/sql
			- 2: with jackc/pgx
			- 3: with pgxpool
			- 4: generate DB load
			- 5: run update with TX
			- 6: generate DB load with PgBouncer`)
	flag.IntVar(&maxConns, "maxConns", 1, "max pool connections")
	flag.IntVar(&minConns, "minConns", 1, "min pool connections")
	flag.Int64Var(&attackMS, "attackMS", 100, "attack duration in MS")
	flag.IntVar(&goroutinesCount, "goroutines", 10, "goroutines count")
	flag.Parse()
	if mode == -1 {
		return nil, fmt.Errorf("running mode must be defined")
	}
	switch mode {
	case 1:
	case 2:
	case 3:
	case 4:
	case 5:
	case 6:
	default:
		return nil, fmt.Errorf("unknown mode %d", mode)
	}
	return &Config{
		Mode:            mode,
		MaxConns:        int32(maxConns),
		MinConns:        int32(minConns),
		AttackMS:        time.Duration(attackMS),
		GoroutinesCount: goroutinesCount,
	}, nil
}

func ComposeConnectionString() string {
	userspec := fmt.Sprintf("%s:%s", PG_USER, PG_PASSWORD)
	hostspec := fmt.Sprintf("%s:%s", PG_HOST, PG_PORT)
	return fmt.Sprintf("postgresql://%s@%s/%s", userspec, hostspec, PG_DB_NAME)
}

type EmpSalaryPGX struct {
	Name     string
	LastName string
	Salary   *pgtype.Numeric
}

func runPGXExample() error {
	connStr := ComposeConnectionString()
	conn, err := pgx.Connect(context.Background(), connStr)
	if err != nil {
		return fmt.Errorf("failed to open a connection to the Database using %s: %w", connStr, err)
	}
	defer conn.Close(context.Background())

	rows, err := conn.Query(context.Background(), sqlGetTwentySalaries, "Alice")
	if err != nil {
		return fmt.Errorf("failed to fetch data from the employees table: %w", err)
	}
	defer rows.Close()
	salaries, err := processPGXRowsToGetSalary(rows)
	if err != nil {
		return fmt.Errorf("failed to process fetched rows: %w", err)
	}
	for _, s := range salaries {
		log.Printf("%s %s: %s", s.Name, s.LastName, s.Salary.Int.String())
	}
	return nil
}

func processPGXRowsToGetSalary(rows pgx.Rows) ([]EmpSalaryPGX, error) {
	salaries := make([]EmpSalaryPGX, 0, 20)
	for rows.Next() {
		var empSalary EmpSalaryPGX
		var salary pgtype.Numeric
		if err := rows.Scan(&empSalary.Name, &empSalary.LastName, &salary); err != nil {
			return nil, fmt.Errorf("failed to scan a returned row: %w", err)
		}
		empSalary.Salary = &salary
		salaries = append(salaries, empSalary)
	}
	return salaries, nil
}

func runPoolExample() error {
	pool, err := createPGXPool(8, 4)
	defer pool.Close()
	rows, err := pool.Query(context.Background(), sqlGetTwentySalaries, "Alice")
	if err != nil {
		return fmt.Errorf("failed to fetch data from the employees table: %w", err)
	}
	salaries, err := processPGXRowsToGetSalary(rows)
	if err != nil {
		return fmt.Errorf("failed to process salary rows: %w", err)
	}
	for _, s := range salaries {
		log.Printf("%s %s: %s", s.Name, s.LastName, s.Salary.Int.String())
	}
	return nil
}

func createPGXPool(maxConns int32, minConns int32) (*pgxpool.Pool, error) {
	cfg, err := getPoolConfig(maxConns, minConns)
	if err != nil {
		return nil, fmt.Errorf("failed to get the pool config: %w", err)
	}
	pool, err := pgxpool.ConnectConfig(context.Background(), cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize a connection pool: %w", err)
	}
	return pool, nil
}

func getPoolConfig(maxConns int32, minConns int32) (*pgxpool.Config, error) {
	connStr := ComposeConnectionString()
	cfg, err := pgxpool.ParseConfig(connStr)
	if err != nil {
		return nil, fmt.Errorf(
			"failed to create a pool config from connection string %s: %w", connStr, err,
		)
	}
	cfg.MaxConns = maxConns
	cfg.MinConns = minConns

	// HealthCheckPeriod - частота проверки работоспособности
	// соединения с Postgres
	cfg.HealthCheckPeriod = 1 * time.Minute

	// MaxConnLifetime - сколько времени будет жить соединение.
	// Так как большого смысла удалять живые соединения нет,
	// можно устанавливать большие значения
	cfg.MaxConnLifetime = 24 * time.Hour

	// MaxConnIdleTime - время жизни неиспользуемого соединения,
	// если запросов не поступало, то соединение закроется.
	cfg.MaxConnIdleTime = 30 * time.Minute

	// ConnectTimeout устанавливает ограничение по времени
	// на весь процесс установки соединения и аутентификации.
	cfg.ConnConfig.ConnectTimeout = 1 * time.Second

	// Лимиты в net.Dialer позволяют достичь предсказуемого
	// поведения в случае обрыва сети.
	cfg.ConnConfig.DialFunc = (&net.Dialer{
		KeepAlive: cfg.HealthCheckPeriod,
		// Timeout на установку соединения гарантирует,
		// что не будет зависаний при попытке установить соединение.
		Timeout: cfg.ConnConfig.ConnectTimeout,
	}).DialContext
	return cfg, nil
}

type (
	EmployeeID   uint32
	DepartmentID uint32
	PositionID   uint32
	Phone        string
	Email        string
)

type Employee struct {
	ID         EmployeeID
	FirstName  string
	LastName   string
	Phone      Phone
	Email      Email
	Salary     *pgtype.Numeric
	ManagerID  EmployeeID
	Department DepartmentID
	Position   PositionID
	EntryAt    *pgtype.Date
}

func InsertEmployee(ctx context.Context, dbpool *pgxpool.Pool, employee *Employee) (EmployeeID, error) {
	const sql = `
INSERT INTO employees
	(first_name, last_name, phone, email, salary, manager_id, department, position, entry_at)
VALUES
	($1, $2, $3, $4, $5, $6, $7, $8, $9::date)
returning id;`

	// При insert разумно использовать метод dbpool.Exec,
	// который не требует возврата данных из запроса.
	// В данном случае после вставки строки мы получаем её идентификатор.
	// Идентификатор вставленной строки может быть использован
	// в интерфейсе приложения.

	var id EmployeeID
	err := dbpool.QueryRow(ctx, sql,
		// Параметры должны передаваться в том порядке,
		// в котором перечислены столбцы в SQL запросе.
		employee.FirstName,
		employee.LastName,
		employee.Phone,
		employee.Email,
		employee.Position,
		employee.Department,
		employee.Salary,
		employee.ManagerID,
		employee.EntryAt,
	).Scan(&id)
	if err != nil {
		return 0, fmt.Errorf("failed to insert employee %v: %w", employee, err)
	}

	return id, nil
}

type EmailSearchHint struct {
	FirstName string
	LastName  string
	Email     Email
	Phone     Phone
}

func SearchPhoneByEmail(
	ctx context.Context,
	dbpool *pgxpool.Pool,
	emailPrefix string,
	limit int,
) ([]EmailSearchHint, error) {
	const sql = `
SELECT first_name, last_name, email, phone
FROM employees
WHERE email LIKE $1
ORDER BY email ASC
LIMIT $2;`
	rows, err := dbpool.Query(ctx, sql, emailPrefix+"%", limit)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch the email search hints: %w", err)
	}
	defer rows.Close()
	hints := make([]EmailSearchHint, 0)
	for rows.Next() {
		h := EmailSearchHint{}
		if err := rows.Scan(&h.FirstName, &h.LastName, &h.Email, &h.Phone); err != nil {
			return nil, fmt.Errorf("failed to scan the received email hint: %w", err)
		}
		hints = append(hints, h)
	}
	return hints, nil
}

func generateDBLoad(cfg *Config) error {
	pool, err := createPGXPool(cfg.MaxConns, cfg.MinConns)
	if err != nil {
		return fmt.Errorf("failed to create a PGX pool: %w", err)
	}
	res, err := Attack(
		context.Background(),
		time.Millisecond*cfg.AttackMS,
		cfg.GoroutinesCount,
		pool,
	)
	if err != nil {
		return fmt.Errorf("attack failed: %w", err)
	}
	log.Println("Attack result: ", res)
	return nil
}

type AttackResults struct {
	Duration         time.Duration
	Threads          int
	QueriesPerformed uint64
}

func (r *AttackResults) String() string {
	return fmt.Sprintf(
		"duration: %dms, threads: %d, queries performed: %d",
		(r.Duration / time.Millisecond),
		r.Threads,
		r.QueriesPerformed,
	)
}

func Attack(
	ctx context.Context,
	duration time.Duration,
	threads int,
	dbpool *pgxpool.Pool,
) (*AttackResults, error) {
	emps, err := readEmps()
	if err != nil {
		return nil, fmt.Errorf("failed to read the employees file: %w", err)
	}
	emailPrefixes := NewEmailPrefixes(emps)

	var queries uint64
	attacker := func(stopAt time.Time) {
		for {
			_, err := SearchPhoneByEmail(ctx, dbpool, emailPrefixes.Get(), 5)
			if err != nil {
				log.Printf("an error occurred while searching by email: %v", err)
				continue
			}
			atomic.AddUint64(&queries, 1)
			if time.Now().After(stopAt) {
				return
			}
		}
	}

	startAt := time.Now()
	stopAt := startAt.Add(duration)

	var wg sync.WaitGroup
	for i := 0; i < threads; i++ {
		wg.Add(1)
		go func() {
			attacker(stopAt)
			wg.Done()
		}()
	}

	wg.Wait()

	return &AttackResults{
		Duration:         time.Now().Sub(startAt),
		Threads:          threads,
		QueriesPerformed: queries,
	}, nil
}

type Emps struct {
	Names     []string `json:"names"`
	LastNames []string `json:"surnames"`
}

func readEmps() (*Emps, error) {
	empsContents, err := os.ReadFile("emps.json")
	if err != nil {
		return nil, fmt.Errorf("failed to open the data file: %w", err)
	}
	var emps Emps
	if err := json.Unmarshal(empsContents, &emps); err != nil {
		return nil, fmt.Errorf("failed to unmrashal the emps data: %w", err)
	}
	return &emps, nil
}

func (e *Emps) Name() string {
	return getRandStrEl(e.Names)
}

func (e *Emps) LastName() string {
	return getRandStrEl(e.LastNames)
}

func (e *Emps) getRandEmailPrefix() string {
	name, lastName := e.Name(), e.LastName()
	nameConcat := string(name[0]) + lastName
	return nameConcat[:getRandIntInInterval(1, len(nameConcat)-1)]
}

func getRandStrEl(sl []string) string {
	return sl[rand.Intn(len(sl))]
}

func getRandIntInInterval(min int, max int) int {
	return rand.Intn(max-min) + min
}

type EmailPrefixes struct {
	prefixes    []string
	prefixesMux *sync.RWMutex
}

const prefixesLen = 1000

func NewEmailPrefixes(emps *Emps) *EmailPrefixes {
	prefixes := make([]string, prefixesLen)
	for i := 0; i < prefixesLen; i++ {
		prefixes[i] = emps.getRandEmailPrefix()
	}
	return &EmailPrefixes{
		prefixes:    prefixes,
		prefixesMux: &sync.RWMutex{},
	}
}

func (ep *EmailPrefixes) Get() string {
	ep.prefixesMux.RLock()
	defer ep.prefixesMux.RUnlock()
	return ep.prefixes[rand.Intn(prefixesLen)]
}

type TransactionFunc func(context.Context, pgx.Tx) error

func inTx(
	ctx context.Context,
	dbpool *pgxpool.Pool,
	f TransactionFunc,
) (err error) {
	transaction, err := dbpool.Begin(ctx)
	if err != nil {
		err = fmt.Errorf("failed to create a transaction: %w", err)
		return
	}

	defer func() {
		if err != nil {
			rbErr := transaction.Rollback(ctx)
			if rbErr != nil {
				log.Print(rbErr)
			}
		}
	}()

	err = f(ctx, transaction)
	if err != nil {
		return
	}
	err = transaction.Commit(ctx)
	if err != nil {
		err = fmt.Errorf("failed to commit the transaction: %w", err)
		return
	}
	return nil
}

func UpdateWithTX(
	ctx context.Context,
	dbpool *pgxpool.Pool,
	id1 EmployeeID,
	id2 EmployeeID,
	salary int,
) error {
	err := inTx(ctx, dbpool, func(ctx context.Context, tx pgx.Tx) error {
		const sql = `update employees set salary = salary+($1) where id = $2;`

		_, err := tx.Exec(ctx, sql, salary, id1)
		if err != nil {
			return err
		}

		_, err = tx.Exec(ctx, sql, -salary, id2)
		if err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return err
	}
	return nil
}

func runUpdateWithTX() error {
	pool, err := createPGXPool(8, 4)
	if err != nil {
		return fmt.Errorf("failed to create a PGX pool: %w", err)
	}
	if err := UpdateWithTX(context.Background(), pool, 3, 4, 100); err != nil {
		return fmt.Errorf("failed to update with tx: %w", err)
	}
	log.Println("updated successfully")
	return nil
}

func createPGXPoolModeDescribe(maxConns int32, minConns int32) (*pgxpool.Pool, error) {
	cfg, err := getPoolConfig(maxConns, minConns)
	if err != nil {
		return nil, fmt.Errorf("failed to get the pool config: %w", err)
	}
	cfg.ConnConfig.BuildStatementCache = func(conn *pgconn.PgConn) stmtcache.Cache {
		mode := stmtcache.ModeDescribe
		capacity := 512
		return stmtcache.New(conn, mode, capacity)
	}
	pool, err := pgxpool.ConnectConfig(context.Background(), cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize a connection pool: %w", err)
	}
	return pool, nil
}

func generateDBLoadWithPgBouncer(cfg *Config) error {
	pool, err := createPGXPoolModeDescribe(cfg.MaxConns, cfg.MinConns)
	if err != nil {
		return fmt.Errorf("failed to create a PGX pool: %w", err)
	}
	res, err := Attack(
		context.Background(),
		time.Millisecond*cfg.AttackMS,
		cfg.GoroutinesCount,
		pool,
	)
	if err != nil {
		return fmt.Errorf("attack failed: %w", err)
	}
	log.Println("Attack result: ", res)
	return nil
}
