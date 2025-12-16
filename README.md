# Go Job Queue

A production-ready, distributed job queue system built with Go, PostgreSQL, and Redis. Designed for reliability, scalability, and at-least-once delivery semantics.

## Table of Contents

- [System Overview](#system-overview)
- [Architecture](#architecture)
- [Worker Flow](#worker-flow)
- [Failure Handling](#failure-handling)
- [Design Decisions](#design-decisions)
- [Configuration](#configuration)
- [Setup Instructions](#setup-instructions)
- [API Documentation](#api-documentation)

## System Overview

This job queue system provides:

- **At-least-once delivery**: Jobs are guaranteed to be processed at least once, with idempotency support to handle duplicates
- **Distributed locking**: Redis-based locks prevent concurrent processing of the same job
- **Automatic retries**: Exponential backoff retry mechanism with configurable max retries
- **Graceful shutdown**: Workers finish processing current jobs before shutting down
- **Scheduled jobs**: Support for delayed and scheduled job execution
- **Idempotency**: Duplicate job detection using idempotency keys

## Architecture

The system consists of two main components:

### 1. API Server (`cmd/api/main.go`)

- HTTP REST API for creating and managing jobs
- Handles job creation, status queries, and cancellation
- Stores jobs in PostgreSQL
- Enqueues jobs to Redis queue

### 2. Worker (`cmd/worker/main.go`)

- Worker pool with configurable number of workers
- Pulls jobs from Redis queue
- Processes jobs using distributed locks
- Handles retries and failures
- Scheduler for processing scheduled jobs

### Component Structure

```
/cmd
  /api/main.go        → HTTP server, DB, Redis initialization, graceful shutdown
  /worker/main.go     → Worker pool initialization, signal handling, blocks until shutdown

/internal
  /job                → Domain models (Job, Status, Retry, Idempotency)
  /storage            → PostgreSQL repository (atomic updates)
  /queue              → Redis queue (Enqueue, Dequeue, Delayed queue)
  /lock               → Redis lock (separate from queue, owner-based)
  /worker              → Worker pool, job processing, handler interface
  /retry               → Pure backoff calculation function
  /scheduler           → Scheduled job enqueuer
  /api                 → HTTP handlers and routing

/migrations
  001_create_jobs.sql → Database schema
```

## Worker Flow

The worker follows a strict, well-defined flow for processing each job:

```mermaid
sequenceDiagram
    participant D as Dispatcher
    participant Q as Redis Queue
    participant W as Worker
    participant L as Redis Lock
    participant DB as PostgreSQL
    participant H as Handler

    D->>Q: Dequeue job_id
    Q-->>D: job_id
    D->>W: Send job_id via channel
    W->>L: AcquireLock(job_id, TTL)
    L-->>W: owner_id, acquired
    W->>DB: Load job
    DB-->>W: job data
    W->>DB: Idempotency check
    DB-->>W: existing job or nil
    alt Idempotency key exists and job DONE
        W->>DB: Mark current job as DONE (duplicate)
        W->>L: ReleaseLock(owner_id)
    else No duplicate
        W->>DB: UpdateStatusAtomic(Pending → Processing)
        DB-->>W: updated
        W->>H: ProcessJob(ctx with timeout)
        alt Handler succeeds
            H-->>W: nil
            W->>DB: UpdateStatusAtomic(Processing → Done)
            W->>L: ReleaseLock(owner_id)
        else Handler fails
            H-->>W: error
            W->>DB: IncrementRetryCount
            alt Can retry
                W->>DB: UpdateStatus(Pending)
                W->>Q: EnqueueDelayed(backoff delay)
                W->>L: ReleaseLock(owner_id)
            else Max retries exceeded
                W->>DB: UpdateStatusAtomic(Processing → Failed)
                W->>L: ReleaseLock(owner_id)
            end
        end
    end
```

### Flow Steps (Mandatory Order)

1. **Pull job_id**: Dispatcher dequeues job ID from Redis queue
2. **Acquire lock**: Worker acquires distributed lock with TTL (prevents concurrent processing)
3. **Load job**: Fetch job data from PostgreSQL
4. **Idempotency check**: Verify if job with same idempotency_key already completed
5. **Mark PROCESSING**: Atomically update status from Pending to Processing
6. **Execute handler**: Run job handler with per-job timeout context
7. **Final status**: Update to DONE, RETRY (enqueue delayed), or FAILED
8. **Release lock**: Release lock using owner ID (ensures only owner can release)

## Failure Handling

### Worker Crashes Mid-Job

**Scenario**: Worker crashes after acquiring lock but before completing job.

**Handling**:

- Lock has TTL (default 5 minutes), so it expires automatically
- Job status remains PROCESSING
- Another worker can acquire lock after TTL expires
- Worker checks status before processing - if already DONE, skips
- If PROCESSING but lock expired, worker can retry (idempotency check prevents duplicates)

**Trade-off**: Lock TTL must be longer than longest expected job execution time.

### Lock Expires Mid-Job

**Scenario**: Job takes longer than lock TTL.

**Handling**:

- Lock expires, another worker can acquire lock
- Original worker's handler may still be running
- When handler completes, status update may fail (lock already released)
- New worker will see PROCESSING status and attempt to process
- Idempotency check prevents duplicate execution if job already completed

**Mitigation**: Set lock TTL longer than max job timeout, or implement lock renewal for long-running jobs.

### Duplicate Delivery (At-Least-Once)

**Scenario**: Network issues cause job to be delivered multiple times.

**Handling**:

- Each job can have an `idempotency_key`
- Before processing, worker checks if job with same key already completed
- If found and DONE, current job is marked as DONE (duplicate) without execution
- Prevents duplicate side effects

**Trade-off**: Requires idempotency keys to be provided by clients. Jobs without keys may execute multiple times.

### Database Connection Loss

**Scenario**: PostgreSQL connection fails during job processing.

**Handling**:

- All database operations use context with timeout
- Errors are logged and job remains in current status
- Retry mechanism will retry failed jobs
- Connection pool automatically reconnects

### Redis Connection Loss

**Scenario**: Redis becomes unavailable.

**Handling**:

- Queue operations fail, dispatcher logs errors and retries
- Lock operations fail, workers skip jobs that can't acquire locks
- System degrades gracefully - no data loss (jobs remain in PostgreSQL)
- When Redis recovers, workers resume processing

## Design Decisions

### Why At-Least-Once Delivery?

**Trade-off**: We chose at-least-once over exactly-once for simplicity and performance.

**Reasons**:

1. **Simplicity**: Exactly-once requires complex distributed transactions and consensus protocols
2. **Performance**: At-least-once avoids coordination overhead
3. **Idempotency**: Application-level idempotency handles duplicates effectively
4. **Industry standard**: Most production systems use at-least-once (e.g., Kafka, SQS)

**Handling duplicates**:

- Idempotency keys for critical operations
- Idempotent handlers (safe to retry)
- Status checks prevent reprocessing completed jobs

### Why Redis?

**Queue**:

- Fast in-memory operations (LPUSH, BRPOP)
- Built-in blocking operations (BRPOP with timeout)
- Sorted sets for delayed jobs (ZADD, ZRANGEBYSCORE)
- Atomic operations (pipeline for batch moves)

**Lock**:

- SETNX for atomic lock acquisition
- TTL for automatic expiration
- Lua scripts for atomic owner verification
- Separate from queue (different concerns)

**Trade-off**: Redis is single-threaded, but provides excellent performance for queue and lock operations. For higher throughput, consider Redis Cluster.

### Why Go?

**Reasons**:

1. **Concurrency**: Goroutines provide efficient concurrent job processing
2. **Performance**: Compiled language with low latency
3. **Context**: Built-in context package for cancellation and timeouts
4. **Standard library**: Rich standard library (HTTP, JSON, etc.)
5. **Deployment**: Single binary, easy to deploy

**Trade-off**: Go's simplicity means less abstraction compared to higher-level languages, but provides better control and performance.

### Lock Expiration Handling

**What happens if lock expires mid-job?**

1. Lock expires after TTL (default 5 minutes)
2. Another worker can acquire lock for same job
3. Original worker's handler may still be running
4. When handler completes:
   - If job completed successfully: Status update may fail (lock released), but job is DONE
   - If job failed: Status update may fail, job remains PROCESSING
5. New worker sees PROCESSING status:
   - If idempotency check finds DONE job: Marks as duplicate
   - Otherwise: Retries job (safe due to idempotency)

**Mitigation**:

- Set lock TTL > max job timeout
- Use idempotency keys for critical operations
- Implement lock renewal for long-running jobs (future enhancement)

### Duplicate Job Handling

**Mechanism**:

1. Client provides `idempotency_key` when creating job
2. API checks if job with same key exists - if yes, returns existing job
3. Worker checks idempotency before processing:
   - Queries database for job with same key
   - If found and DONE: Marks current job as DONE (duplicate)
   - Otherwise: Processes normally

**When duplicates occur**:

- Network retries cause duplicate API calls → API returns existing job
- Queue delivers same job twice → Worker idempotency check prevents duplicate execution
- Worker crashes and restarts → Lock prevents concurrent execution, status check prevents reprocessing

**Trade-off**: Requires clients to provide idempotency keys. Jobs without keys may execute multiple times.

## Configuration

### Environment Variables

#### API Server (`cmd/api/main.go`)

- `DATABASE_URL`: PostgreSQL connection string (default: `postgres://user:password@localhost:5432/jobqueue?sslmode=disable`)
- `REDIS_ADDR`: Redis address (default: `localhost:6379`)
- `REDIS_PASSWORD`: Redis password (default: empty)
- `API_ADDR`: HTTP server address (default: `:8080`)

#### Worker (`cmd/worker/main.go`)

- `DATABASE_URL`: PostgreSQL connection string (default: `postgres://user:password@localhost:5432/jobqueue?sslmode=disable`)
- `REDIS_ADDR`: Redis address (default: `localhost:6379`)
- `REDIS_PASSWORD`: Redis password (default: empty)
- `NUM_WORKERS`: Number of worker goroutines (default: `5`)
- `LOCK_TTL`: Lock time-to-live duration (default: `5m`)
- `JOB_TIMEOUT`: Per-job execution timeout (default: `5m`)

### Default Values

- Workers: 5
- Lock TTL: 5 minutes
- Job Timeout: 5 minutes
- Max Retries: 3 (per job, configurable)
- Base Backoff: 1 second
- Max Backoff: 5 minutes

## Setup Instructions

### Prerequisites

- Go 1.21 or later
- PostgreSQL 12 or later
- Redis 6 or later
- Docker and Docker Compose (optional, for local development)

### Local Development with Docker Compose

1. **Start dependencies**:

```bash
docker-compose up -d
```

This starts PostgreSQL and Redis containers.

2. **Run migrations**:

```bash
psql postgres://user:password@localhost:5432/jobqueue -f migrations/001_create_jobs.sql
```

3. **Start API server**:

```bash
go run cmd/api/main.go
```

4. **Start worker** (in separate terminal):

```bash
go run cmd/worker/main.go
```

### Manual Setup

1. **Start PostgreSQL** and create database:

```sql
CREATE DATABASE jobqueue;
```

2. **Run migrations**:

```bash
psql postgres://user:password@localhost:5432/jobqueue -f migrations/001_create_jobs.sql
```

3. **Start Redis**:

```bash
redis-server
```

4. **Set environment variables** (optional):

```bash
export DATABASE_URL="postgres://user:password@localhost:5432/jobqueue?sslmode=disable"
export REDIS_ADDR="localhost:6379"
export NUM_WORKERS=10
export LOCK_TTL=10m
export JOB_TIMEOUT=10m
```

5. **Start services**:

```bash
# Terminal 1: API server
go run cmd/api/main.go

# Terminal 2: Worker
go run cmd/worker/main.go
```

### Building Binaries

```bash
# Build API server
go build -o bin/api cmd/api/main.go

# Build worker
go build -o bin/worker cmd/worker/main.go
```

## API Documentation

### Base URL

```
http://localhost:8080
```

### Endpoints

#### Create Job

**POST** `/api/v1/jobs`

Creates a new job and enqueues it for processing.

**Request Body**:

```json
{
  "type": "send_email",
  "payload": {
    "to": "user@example.com",
    "subject": "Welcome",
    "body": "Hello!"
  },
  "max_retries": 3,
  "scheduled_at": "2024-01-01T00:00:00Z",
  "idempotency_key": "unique-key-123"
}
```

**Response** (201 Created):

```json
{
  "id": "550e8400-e29b-41d4-a716-446655440000",
  "status": "Pending",
  "type": "send_email",
  "payload": {
    "to": "user@example.com",
    "subject": "Welcome",
    "body": "Hello!"
  }
}
```

**Idempotency**: If `idempotency_key` is provided and a job with the same key exists, returns the existing job (200 OK).

#### Get Job

**GET** `/api/v1/jobs/{id}`

Retrieves job status and details.

**Response** (200 OK):

```json
{
  "id": "550e8400-e29b-41d4-a716-446655440000",
  "type": "send_email",
  "payload": {...},
  "status": "Done",
  "retry_count": 0,
  "max_retries": 3,
  "scheduled_at": null,
  "idempotency_key": "unique-key-123",
  "created_at": "2024-01-01T00:00:00Z",
  "updated_at": "2024-01-01T00:00:10Z"
}
```

**Status Values**:

- `Pending`: Job is queued, waiting to be processed
- `Processing`: Job is currently being processed
- `Done`: Job completed successfully
- `Failed`: Job failed after max retries

#### Cancel Job

**POST** `/api/v1/jobs/{id}/cancel`

Cancels a pending or processing job.

**Response** (200 OK):

```json
{
  "message": "Job cancelled successfully",
  "job_id": "550e8400-e29b-41d4-a716-446655440000"
}
```

**Note**: Only Pending or Processing jobs can be cancelled. Completed or Failed jobs cannot be cancelled.

#### Health Check

**GET** `/health`

Returns 200 OK if server is healthy.

## Job Handler Implementation

To implement custom job handlers, implement the `JobHandler` interface:

```go
type JobHandler interface {
    ProcessJob(ctx context.Context, job *job.Job) error
}
```

**Important**:

- Handler should only return errors, not retry or sleep
- Handler must be idempotent (safe to retry)
- Handler should respect context cancellation (timeout)
- Handler should not use `time.Sleep` - use context for cancellation

**Example**:

```go
type EmailHandler struct{}

func (h *EmailHandler) ProcessJob(ctx context.Context, j *job.Job) error {
    var payload EmailPayload
    if err := json.Unmarshal(j.Payload, &payload); err != nil {
        return err
    }

    // Check context cancellation
    if ctx.Err() != nil {
        return ctx.Err()
    }

    // Send email (idempotent operation)
    return sendEmail(payload.To, payload.Subject, payload.Body)
}
```

## Monitoring and Observability

### Logging

All components use structured JSON logging with the following fields:

- `job_id`: Job identifier
- `worker_id`: Worker identifier
- `lock_owner`: Lock owner ID
- `retry_count`: Current retry count
- `error`: Error details

### Key Metrics to Monitor

1. **Queue depth**: Number of jobs in Redis queue
2. **Processing time**: Time from enqueue to completion
3. **Retry rate**: Percentage of jobs that require retries
4. **Failure rate**: Percentage of jobs that fail after max retries
5. **Lock acquisition failures**: Rate of lock acquisition failures
6. **Worker utilization**: Percentage of workers actively processing jobs

## Limitations and Future Enhancements

### Current Limitations

1. **No lock renewal**: Long-running jobs may exceed lock TTL
2. **No priority queues**: All jobs have equal priority
3. **No job dependencies**: Cannot chain jobs
4. **No metrics endpoint**: No Prometheus/metrics export
5. **Single Redis instance**: No Redis Cluster support

### Future Enhancements

1. Lock renewal for long-running jobs
2. Priority queues
3. Job dependencies and workflows
4. Prometheus metrics export
5. Redis Cluster support
6. Dead letter queue for permanently failed jobs
7. Job result storage
8. Web UI dashboard

## License

See LICENSE file for details.
