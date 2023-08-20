package pgq

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/james-elicx/go-utils/utils"
)

const (
	JobStatusWaiting = "waiting" // Job is waiting to be processed
	JobStatusRunning = "running" // Job is currently being processed
	JobStatusDone    = "done"    // Job has been processed successfully
	JobStatusError   = "error"   // Job has been processed and resulted in an error
)

var TableName = "__pgq_jobs" // Name of the table used to store jobs

// Queue is a queue of jobs.
type Queue struct {
	db       *sql.DB
	handlers map[string]func(job Job) error
}

// NewQueue creates a new queue with the given database.
func NewQueue(db *sql.DB) *Queue {
	return &Queue{
		db:       db,
		handlers: make(map[string]func(job Job) error),
	}
}

// Job is a job in the queue.
type Job struct {
	ID         int            // Job ID
	Type       string         // Type of job
	Data       string         // Data for the job
	Status     string         // Job status
	Error      sql.NullString // Error message if the job failed
	Attempt    int            // Number of times the job has been attempted
	CreatedAt  time.Time      // When the job was created
	StartedAt  sql.NullTime   // When the job was started
	FinishedAt sql.NullTime   // When the job was finished
}

// SetupDatabase sets up the database for the queue, creating the table and indexes if they don't exist.
func (q *Queue) SetupDatabase(ctx context.Context) error {
	if _, err := q.db.ExecContext(ctx, fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %[1]s (
			id SERIAL		PRIMARY KEY,

			job_type		TEXT NOT NULL,
			data				TEXT NOT NULL,

			status			TEXT NOT NULL DEFAULT '%[2]s',
			error				TEXT,
			attempt 		INT NOT NULL DEFAULT 0,

			created_at	TIMESTAMP NOT NULL DEFAULT NOW(),
			started_at	TIMESTAMP,
			finished_at TIMESTAMP
		);

		CREATE INDEX IF NOT EXISTS idx_%[1]s_status ON %[1]s(status);
	`, TableName, JobStatusWaiting)); err != nil {
		return fmt.Errorf("queue: failed to setup database: %w", err)
	}

	return nil
}

// RegisterHandler registers a handler to the queue for the given job type.
//
// The handler will be called when a job of the given type is processed. If the handler returns
// an error, the job will be marked as failed and the error will be stored in the database.
func (q *Queue) RegisterHandler(jobType string, handler func(job Job) error) error {
	if _, ok := q.handlers[jobType]; ok {
		return fmt.Errorf("queue: handler already registered for job type %s", jobType)
	}

	q.handlers[jobType] = handler
	return nil
}

// Put adds a job to the queue with the given job type and data.
func (q *Queue) Put(ctx context.Context, jobType string, data string) error {
	if _, ok := q.handlers[jobType]; !ok {
		return fmt.Errorf("queue: no handler registered for job type %s", jobType)
	}

	if _, err := q.db.ExecContext(ctx, fmt.Sprintf(`
		INSERT INTO %s (job_type, data) VALUES ($1, $2);
	`, TableName), jobType, data); err != nil {
		return fmt.Errorf("queue: failed to add job: %w", err)
	}

	return nil
}

// Pop pops a job from the queue that matches one of the given job types, and processes it using
// the registered handler for the job type.
func (q *Queue) Pop(ctx context.Context, jobTypes []string) error {
	if len(jobTypes) == 0 {
		return fmt.Errorf("queue: no job type specified")
	}

	for _, jobType := range jobTypes {
		if _, ok := q.handlers[jobType]; !ok {
			return fmt.Errorf("queue: no handler registered for job type %s", jobType)
		}
	}

	tx, err := q.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("queue: failed to start transaction: %w", err)
	}
	defer tx.Rollback()

	row := tx.QueryRowContext(ctx, fmt.Sprintf(`
		UPDATE %[1]s
		SET
			status = $1,
			error = NULL,
			attempt = attempt + 1,
			started_at = NOW(),
			finished_at = NULL
		WHERE id IN (
			SELECT id FROM %[1]s AS jobs
			WHERE jobs.status = $2 AND jobs.job_type = ANY($3)
			ORDER BY jobs.id ASC
			FOR UPDATE SKIP LOCKED
			LIMIT 1
		)
		RETURNING id, job_type, data, status, error, attempt, created_at, started_at, finished_at
	`, TableName), JobStatusRunning, JobStatusWaiting, jobTypes)

	var job Job

	if err = row.Scan(&job.ID, &job.Type, &job.Data, &job.Status, &job.Error, &job.Attempt, &job.CreatedAt, &job.StartedAt, &job.FinishedAt); err == sql.ErrNoRows {
		return nil // should this return an error instead?
	} else if err != nil {
		return fmt.Errorf("queue: failed to pop job: %w", err)
	}

	err = q.handlers[job.Type](job)

	newStatus := utils.Ternary(err == nil, JobStatusDone, JobStatusError)
	var newError *string
	if err != nil {
		newErrorStr := fmt.Sprintf("%s", err.Error())
		newError = &newErrorStr
	}

	if _, err := tx.ExecContext(ctx, fmt.Sprintf(`
		UPDATE %s
		SET
			status = $1,
			error = $2,
			finished_at = NOW()
		WHERE id = $3
	`, TableName), newStatus, newError, job.ID); err != nil {
		return fmt.Errorf("queue: failed to update job status: %w", err)
	}

	if err = tx.Commit(); err != nil {
		return fmt.Errorf("queue: failed to commit transaction: %w", err)
	}
	return nil
}
