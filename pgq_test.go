package pgq_test

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"testing"

	embeddedpostgres "github.com/fergusstrange/embedded-postgres"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
	a "github.com/james-elicx/go-utils/assert"
	"github.com/james-elicx/pgq"
)

var db *sql.DB

func TestMain(m *testing.M) {
	var database *embeddedpostgres.EmbeddedPostgres

	connUrl := os.Getenv("TEST_POSTGRES_URL")
	if connUrl == "" {
		cfg := embeddedpostgres.DefaultConfig()
		connUrl = cfg.GetConnectionURL()

		database = embeddedpostgres.NewDatabase(cfg)
		if err := database.Start(); err != nil {
			panic(err)
		}
	}

	config, err := pgx.ParseConfig(connUrl)
	if err != nil {
		panic(err)
	}

	db = stdlib.OpenDB(*config)

	code := m.Run()

	if database != nil {
		if err := database.Stop(); err != nil {
			panic(err)
		}
	}

	os.Exit(code)
}

func setupQueue(t *testing.T) (*pgq.Queue, context.Context) {
	q := pgq.NewQueue(db)

	ctx := context.Background()
	if err := q.SetupDatabase(ctx); err != nil {
		t.Fatal(err)
	}

	return q, ctx
}

func TestRegisterHandler(t *testing.T) {
	q, _ := setupQueue(t)

	handler := func(job pgq.Job) error {
		return nil
	}

	// passes when no handler is registered for job type
	err := q.RegisterHandler("test_type", handler)
	a.Equals(t, err, nil)

	// fails when handler is already registered
	err = q.RegisterHandler("test_type", handler)
	a.EqualsErrorMessage(t, err, "queue: handler already registered for job type test_type")
}

func TestPut(t *testing.T) {
	q, ctx := setupQueue(t)

	handler := func(job pgq.Job) error {
		return nil
	}

	// fails when no handler is registered for job type
	err := q.Put(ctx, "test_type", "test_data")
	a.EqualsErrorMessage(t, err, "queue: no handler registered for job type test_type")

	// register the handler for the job type
	err = q.RegisterHandler("test_type", handler)
	a.Equals(t, err, nil)

	// passes when handler is registered for job type
	err = q.Put(ctx, "test_type", "test_data")
}

func TestPop(t *testing.T) {
	q, ctx := setupQueue(t)

	handler := func(job pgq.Job) error {
		return nil
	}

	// fails when no job type is specified
	err := q.Pop(ctx, []string{})
	a.EqualsErrorMessage(t, err, "queue: no job type specified")

	// fails when no handler is registered for job type
	err = q.Pop(ctx, []string{"test_type"})
	a.EqualsErrorMessage(t, err, "queue: no handler registered for job type test_type")

	// register the handler for the job type
	err = q.RegisterHandler("test_type", handler)
	a.Equals(t, err, nil)

	// pops entry successfully
	err = q.Pop(ctx, []string{"test_type"})
	a.Equals(t, err, nil)

	// updates entry status to done
	var job pgq.Job
	row := db.QueryRowContext(ctx, fmt.Sprintf("SELECT id, job_type, data, status, error, attempt, created_at, started_at, finished_at FROM %s WHERE job_type = 'test_type' ORDER BY id ASC LIMIT 1", pgq.TableName))
	row.Scan(&job.ID, &job.Type, &job.Data, &job.Status, &job.Error, &job.Attempt, &job.CreatedAt, &job.StartedAt, &job.FinishedAt)

	a.Equals(t, job.Type, "test_type")
	a.Equals(t, job.Data, "test_data")
	a.Equals(t, job.Status, pgq.JobStatusDone)
	a.Equals(t, job.Error.Valid, false)
}

func TestPopHandlerError(t *testing.T) {
	q, ctx := setupQueue(t)

	handler := func(job pgq.Job) error {
		return fmt.Errorf("test error")
	}

	// register the handler for the job type
	err := q.RegisterHandler("test_err_type", handler)
	a.Equals(t, err, nil)

	// adds some entries to the queue
	err = q.Put(ctx, "test_err_type", "test_data")
	a.Equals(t, err, nil)

	// pops entry successfully
	err = q.Pop(ctx, []string{"test_err_type"})
	a.Equals(t, err, nil)

	// updates entry status to error and adds error message
	var job pgq.Job
	row := db.QueryRowContext(ctx, fmt.Sprintf("SELECT id, job_type, data, status, error, attempt, created_at, started_at, finished_at FROM %s WHERE job_type = 'test_err_type' ORDER BY id ASC LIMIT 1", pgq.TableName))
	row.Scan(&job.ID, &job.Type, &job.Data, &job.Status, &job.Error, &job.Attempt, &job.CreatedAt, &job.StartedAt, &job.FinishedAt)

	a.Equals(t, job.Type, "test_err_type")
	a.Equals(t, job.Data, "test_data")
	a.Equals(t, job.Status, pgq.JobStatusError)
	a.Equals(t, job.Error.Valid, true)
	a.Equals(t, job.Error.String, "test error")
}
