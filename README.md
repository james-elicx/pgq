<p align="center">
  <h3 align="center">pgq</h3>

  <p align="center">
    Postgres Job Queues
  </p>
</p>

---

This library is a simple way to use Postgres for a job queue system in Golang projects. It takes advantage of the well-known approach of using locks, as described in [this Crunchy Data article](https://www.crunchydata.com/blog/message-queuing-using-native-postgresql).

pgq supports multiple queues through the use of job *types*, meaning that each job is given the type that you specify, and each time you retrieve a job from the queue, you specify the types of jobs that you want to retrieve.

## Installation

To get started with pgq, add the module to your Go project.

```sh
go get github.com/james-elicx/pgq@v1
```

## Usage

```go
import (
  "context"
  
  "github.com/james-elicx/pgq"
)

func main() {
	db := ... // Open a connection to the database
	ctx := context.Background()

	// Create a new queue instance
	q := pgq.NewQueue(db)

	// Setup the database schema
	if err := q.SetupDatabase(ctx); err != nil {
		panic(err)
	}

	// Register a handler for a job type
	if err = q.RegisterHandler("test_type", func(job pgq.Job) error {
		// ... process the job ...
		return nil
	}); err != nil {
		panic(err)
	}

	// Add a job to the queue
	if err = q.Put(ctx, "test_type", "test_data"); err != nil {
		panic(err)
	}

	// Retrieve and process a job from the queue
	if err = q.Pop(ctx, []string{"test_type"}); err != nil {
		panic(err)
	}
}
```