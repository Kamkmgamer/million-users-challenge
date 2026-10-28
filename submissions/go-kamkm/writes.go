package main

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

const writeBatchLimit = 64

type writeKind uint8

const (
	writeCreate writeKind = iota
	writeLike
)

type writeJob struct {
	kind     writeKind
	userID   int64
	username string
	body     string
	postID   int64
	result   chan writeResult
}

type writeResult struct {
	post     postJSON
	inserted bool
	notFound bool
	err      error
}

type writeBatcher struct {
	db   *pgxpool.Pool
	jobs chan writeJob
}

func newWriteBatcher(db *pgxpool.Pool) *writeBatcher {
	b := &writeBatcher{db: db, jobs: make(chan writeJob, writeBatchLimit*4)}
	go b.run()
	return b
}

func (b *writeBatcher) submit(ctx context.Context, job writeJob) writeResult {
	job.result = make(chan writeResult, 1)
	select {
	case b.jobs <- job:
	case <-ctx.Done():
		return writeResult{err: ctx.Err()}
	}
	select {
	case result := <-job.result:
		return result
	case <-ctx.Done():
		return writeResult{err: ctx.Err()}
	}
}

func (b *writeBatcher) run() {
	for first := range b.jobs {
		batch := make([]writeJob, 1, writeBatchLimit)
		batch[0] = first
		timer := time.NewTimer(time.Millisecond)
	collect:
		for len(batch) < writeBatchLimit {
			select {
			case job := <-b.jobs:
				batch = append(batch, job)
			case <-timer.C:
				break collect
			}
		}
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
		b.process(batch)
	}
}

func (b *writeBatcher) process(batch []writeJob) {
	ctx, cancel := context.WithTimeout(context.Background(), databaseTimeout)
	defer cancel()
	tx, err := b.db.Begin(ctx)
	if err != nil {
		for _, job := range batch {
			b.processOne(job)
		}
		return
	}
	results := make([]writeResult, len(batch))
	for i, job := range batch {
		results[i], err = executeWrite(ctx, tx, job)
		if err != nil {
			break
		}
	}
	if err == nil {
		err = tx.Commit(ctx)
	}
	if err == nil {
		for i, job := range batch {
			job.result <- results[i]
		}
		return
	}
	_ = tx.Rollback(ctx)
	for _, job := range batch {
		b.processOne(job)
	}
}

func (b *writeBatcher) processOne(job writeJob) {
	ctx, cancel := context.WithTimeout(context.Background(), databaseTimeout)
	defer cancel()
	tx, err := b.db.Begin(ctx)
	if err != nil {
		job.result <- writeResult{err: err}
		return
	}
	result, err := executeWrite(ctx, tx, job)
	if err == nil {
		err = tx.Commit(ctx)
	}
	if err != nil {
		_ = tx.Rollback(ctx)
		result.err = err
	}
	job.result <- result
}

func executeWrite(ctx context.Context, tx pgx.Tx, job writeJob) (writeResult, error) {
	switch job.kind {
	case writeCreate:
		var result writeResult
		result.post.Body, result.post.Author = job.body, job.username
		var created time.Time
		err := tx.QueryRow(ctx, "INSERT INTO posts (user_id, body) VALUES ($1, $2) RETURNING id, created_at", job.userID, job.body).Scan(&result.post.ID, &created)
		if err != nil {
			return result, err
		}
		result.post.CreatedAt = formatCreated(created)
		return result, nil
	case writeLike:
		tag, err := tx.Exec(ctx, "INSERT INTO likes (user_id, post_id) VALUES ($1, $2) ON CONFLICT (user_id, post_id) DO NOTHING", job.userID, job.postID)
		if err != nil {
			var pgError *pgconn.PgError
			if errors.As(err, &pgError) && pgError.Code == "23503" && pgError.ConstraintName == "likes_post_id_fkey" {
				return writeResult{notFound: true}, err
			}
			return writeResult{}, err
		}
		return writeResult{inserted: tag.RowsAffected() != 0}, nil
	default:
		return writeResult{}, errors.New("unknown write kind")
	}
}
