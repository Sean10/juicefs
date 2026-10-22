/*
 * JuiceFS, Copyright 2026 Juicedata, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package main

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/juicedata/juicefs/pkg/meta"
	"github.com/redis/go-redis/v9"
)

// item is one directory waiting to be listed.
type item struct {
	Inode meta.Ino
	Depth int
	Path  string
}

func (it item) encode() string {
	return fmt.Sprintf("%d\t%d\t%s", it.Inode, it.Depth, it.Path)
}

func decodeItem(s string) (item, error) {
	parts := strings.SplitN(s, "\t", 3)
	if len(parts) != 3 {
		return item{}, fmt.Errorf("bad queue item %q", s)
	}
	ino, err := strconv.ParseUint(parts[0], 10, 64)
	if err != nil {
		return item{}, err
	}
	depth, err := strconv.Atoi(parts[1])
	if err != nil {
		return item{}, err
	}
	return item{meta.Ino(ino), depth, parts[2]}, nil
}

// workQueue is a directory queue shared by all threads of all workers.
// pending counts queued plus in-progress directories: it is incremented before children
// are pushed and decremented after their parent is fully processed, so pending == 0
// means the whole tree has been listed.
type workQueue interface {
	// Seed pushes the root directory once per job; it returns false if another worker did.
	Seed(ctx context.Context, root item) (bool, error)
	// Seeded reports whether the root has been pushed (by any worker).
	Seeded(ctx context.Context) (bool, error)
	Push(ctx context.Context, items []item) error
	// Pop waits up to timeout for a directory; ok is false on timeout.
	Pop(ctx context.Context, timeout time.Duration) (it item, ok bool, err error)
	// Done marks one popped directory as finished.
	Done(ctx context.Context) error
	// Progress returns the number of pending and finished directories of the whole job.
	Progress(ctx context.Context) (pending, done int64, err error)
}

type localQueue struct {
	mu      sync.Mutex
	cond    *sync.Cond
	items   []item
	pending int64
	done    int64
	seeded  bool
}

func newLocalQueue() *localQueue {
	q := &localQueue{}
	q.cond = sync.NewCond(&q.mu)
	return q
}

func (q *localQueue) Seed(ctx context.Context, root item) (bool, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.seeded {
		return false, nil
	}
	q.seeded = true
	q.pending++
	q.items = append(q.items, root)
	q.cond.Broadcast()
	return true, nil
}

func (q *localQueue) Seeded(ctx context.Context) (bool, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.seeded, nil
}

func (q *localQueue) Push(ctx context.Context, items []item) error {
	q.mu.Lock()
	q.pending += int64(len(items))
	q.items = append(q.items, items...)
	q.mu.Unlock()
	q.cond.Broadcast()
	return nil
}

func (q *localQueue) Pop(ctx context.Context, timeout time.Duration) (item, bool, error) {
	deadline := time.Now().Add(timeout)
	q.mu.Lock()
	defer q.mu.Unlock()
	for len(q.items) == 0 {
		if time.Now().After(deadline) {
			return item{}, false, nil
		}
		t := time.AfterFunc(time.Until(deadline), q.cond.Broadcast)
		q.cond.Wait()
		t.Stop()
	}
	it := q.items[len(q.items)-1] // LIFO keeps the queue short (depth first)
	q.items = q.items[:len(q.items)-1]
	return it, true, nil
}

func (q *localQueue) Done(ctx context.Context) error {
	q.mu.Lock()
	q.pending--
	q.done++
	q.mu.Unlock()
	q.cond.Broadcast()
	return nil
}

func (q *localQueue) Progress(ctx context.Context) (int64, int64, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.pending, q.done, nil
}

// redisQueue coordinates workers on different machines through one Redis instance
// (a separate one is recommended, it only holds directory names of the running job).
type redisQueue struct {
	rdb                                  redis.UniversalClient
	list, pending, done, seeded, workers string
	ttl                                  time.Duration
}

func newRedisQueue(url, job string) (*redisQueue, error) {
	opt, err := redis.ParseURL(url)
	if err != nil {
		return nil, err
	}
	prefix := "jfs-inventory:{" + job + "}:"
	return &redisQueue{
		rdb:     redis.NewClient(opt),
		list:    prefix + "queue",
		pending: prefix + "pending",
		done:    prefix + "done",
		seeded:  prefix + "seeded",
		workers: prefix + "workers",
		ttl:     72 * time.Hour,
	}, nil
}

func (q *redisQueue) Seed(ctx context.Context, root item) (bool, error) {
	ok, err := q.rdb.SetNX(ctx, q.seeded, time.Now().UTC().Format(time.RFC3339), q.ttl).Result()
	if err != nil || !ok {
		return false, err
	}
	_, err = q.rdb.TxPipelined(ctx, func(p redis.Pipeliner) error {
		p.IncrBy(ctx, q.pending, 1)
		p.LPush(ctx, q.list, root.encode())
		for _, k := range []string{q.list, q.pending, q.done} {
			p.Expire(ctx, k, q.ttl)
		}
		return nil
	})
	return true, err
}

// WaitWorkers registers this worker and waits until n workers of the job have registered,
// so that pods started a few seconds apart all get a share of the tree.
func (q *redisQueue) WaitWorkers(ctx context.Context, n int64, timeout time.Duration) (int64, error) {
	got, err := q.rdb.Incr(ctx, q.workers).Result()
	if err != nil {
		return 0, err
	}
	q.rdb.Expire(ctx, q.workers, q.ttl)
	deadline := time.Now().Add(timeout)
	for got < n && time.Now().Before(deadline) {
		time.Sleep(500 * time.Millisecond)
		if got, err = q.rdb.Get(ctx, q.workers).Int64(); err != nil {
			return 0, err
		}
	}
	return got, nil
}

func (q *redisQueue) Seeded(ctx context.Context) (bool, error) {
	n, err := q.rdb.Exists(ctx, q.seeded).Result()
	return n == 1, err
}

func (q *redisQueue) Push(ctx context.Context, items []item) error {
	if len(items) == 0 {
		return nil
	}
	vals := make([]interface{}, len(items))
	for i, it := range items {
		vals[i] = it.encode()
	}
	_, err := q.rdb.TxPipelined(ctx, func(p redis.Pipeliner) error {
		p.IncrBy(ctx, q.pending, int64(len(items)))
		p.LPush(ctx, q.list, vals...)
		return nil
	})
	return err
}

func (q *redisQueue) Pop(ctx context.Context, timeout time.Duration) (item, bool, error) {
	if timeout < time.Second {
		timeout = time.Second // BLPOP granularity
	}
	res, err := q.rdb.BLPop(ctx, timeout, q.list).Result()
	if err == redis.Nil {
		return item{}, false, nil
	}
	if err != nil {
		return item{}, false, err
	}
	it, err := decodeItem(res[1])
	return it, err == nil, err
}

func (q *redisQueue) Done(ctx context.Context) error {
	_, err := q.rdb.TxPipelined(ctx, func(p redis.Pipeliner) error {
		p.Decr(ctx, q.pending)
		p.Incr(ctx, q.done)
		return nil
	})
	return err
}

func (q *redisQueue) Progress(ctx context.Context) (int64, int64, error) {
	vals, err := q.rdb.MGet(ctx, q.pending, q.done).Result()
	if err != nil {
		return 0, 0, err
	}
	parse := func(v interface{}) int64 {
		s, _ := v.(string)
		n, _ := strconv.ParseInt(s, 10, 64)
		return n
	}
	return parse(vals[0]), parse(vals[1]), nil
}

// record is one path of the inventory; the schema matches juicefs-inventory.py.
type record struct {
	Volume      string  `json:"volume"`
	Snapshot    string  `json:"snapshot"`
	Inode       uint64  `json:"inode"`
	ParentInode uint64  `json:"parent_inode"`
	Path        string  `json:"path"`
	Name        string  `json:"name"`
	Depth       int     `json:"depth"`
	Type        string  `json:"type"`
	Size        uint64  `json:"size"`
	Mode        string  `json:"mode"`
	Uid         uint32  `json:"uid"`
	Gid         uint32  `json:"gid"`
	Nlink       uint32  `json:"nlink"`
	Atime       string  `json:"atime"`
	Mtime       string  `json:"mtime"`
	Ctime       string  `json:"ctime"`
	Symlink     *string `json:"symlink"`
}

var typeNames = map[uint8]string{
	meta.TypeFile: "regular", meta.TypeDirectory: "directory", meta.TypeSymlink: "symlink",
	meta.TypeFIFO: "fifo", meta.TypeBlockDev: "blockdev", meta.TypeCharDev: "chardev", meta.TypeSocket: "socket",
}

func isoTime(sec int64, nsec uint32) string {
	return time.Unix(sec, int64(nsec)).UTC().Format("2006-01-02T15:04:05.000000+00:00")
}

type options struct {
	Volume       string
	Snapshot     string
	RootPath     string // path of the listed root inside the volume, e.g. "/" or the subdir
	Threads      int
	PopTimeout   time.Duration
	StallTimeout time.Duration
	LogInterval  time.Duration
	Worker       string
}

type stats struct {
	dirs, records, bytes atomic.Int64
}

// run lists directories from q with opt.Threads goroutines until the whole job is done,
// writing one JSON record per entry to w.
func run(ctx context.Context, m meta.Meta, q workQueue, w io.Writer, opt options) (*stats, error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	if _, err := q.Seed(ctx, item{meta.RootInode, 0, strings.TrimSuffix(opt.RootPath, "/")}); err != nil {
		return nil, fmt.Errorf("seed queue: %w", err)
	}

	out := make(chan []byte, 1024)
	bw := bufio.NewWriterSize(w, 1<<20)
	writeErr := make(chan error, 1)
	go func() {
		var err error
		for b := range out {
			if err == nil {
				_, err = bw.Write(b)
			}
		}
		if err == nil {
			err = bw.Flush()
		}
		writeErr <- err
	}()

	st := &stats{}
	var firstErr error
	var errOnce sync.Once
	fail := func(err error) {
		errOnce.Do(func() { firstErr = err; cancel() })
	}

	var wg sync.WaitGroup
	for i := 0; i < opt.Threads; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := worker(ctx, m, q, out, opt, st); err != nil {
				fail(err)
			}
		}()
	}
	done := make(chan struct{})
	go func() {
		ticker := time.NewTicker(opt.LogInterval)
		defer ticker.Stop()
		for {
			select {
			case <-done:
				return
			case <-ticker.C:
				pending, finished, _ := q.Progress(ctx)
				log.Printf("worker %s: %d dirs, %d records listed here; job: %d dirs done, %d pending",
					opt.Worker, st.dirs.Load(), st.records.Load(), finished, pending)
			}
		}
	}()
	wg.Wait()
	close(done)
	close(out)
	if err := <-writeErr; err != nil && firstErr == nil {
		firstErr = err
	}
	return st, firstErr
}

func worker(ctx context.Context, m meta.Meta, q workQueue, out chan<- []byte, opt options, st *stats) error {
	lastDone, lastChange := int64(-1), time.Now()
	for {
		it, ok, err := q.Pop(ctx, opt.PopTimeout)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("pop: %w", err)
		}
		if !ok {
			seeded, err := q.Seeded(ctx)
			if err != nil {
				return err
			}
			if !seeded {
				continue
			}
			pending, finished, err := q.Progress(ctx)
			if err != nil {
				return err
			}
			if pending <= 0 {
				return nil
			}
			// Directories are in progress elsewhere; give up only if the job stopped moving,
			// e.g. a worker died while holding directories.
			if finished != lastDone {
				lastDone, lastChange = finished, time.Now()
			} else if time.Since(lastChange) > opt.StallTimeout {
				return fmt.Errorf("no progress for %s with %d directories pending; a worker probably died, rerun with a new job id", opt.StallTimeout, pending)
			}
			continue
		}
		if err := listDir(ctx, m, q, out, it, opt, st); err != nil {
			return err
		}
		if err := q.Done(ctx); err != nil {
			return fmt.Errorf("done: %w", err)
		}
	}
}

func listDir(ctx context.Context, m meta.Meta, q workQueue, out chan<- []byte, it item, opt options, st *stats) error {
	var entries []*meta.Entry
	if eno := m.Readdir(meta.Background(), it.Inode, 1, &entries); eno != 0 {
		if eno == syscall.ENOENT {
			return nil // removed while the inventory was running
		}
		return fmt.Errorf("readdir %s (inode %d): %s", it.Path, it.Inode, eno)
	}
	var subdirs []item
	for _, e := range entries {
		name := string(e.Name)
		if name == "." || name == ".." || e.Inode >= meta.TrashInode {
			continue
		}
		a := e.Attr
		p := it.Path + "/" + name
		r := record{
			Volume: opt.Volume, Snapshot: opt.Snapshot, Inode: uint64(e.Inode), ParentInode: uint64(it.Inode),
			Path: p, Name: name, Depth: it.Depth + 1, Type: typeNames[a.Typ],
			Mode: fmt.Sprintf("%04o", a.Mode&0o7777), Uid: a.Uid, Gid: a.Gid, Nlink: a.Nlink,
			Atime: isoTime(a.Atime, a.Atimensec), Mtime: isoTime(a.Mtime, a.Mtimensec), Ctime: isoTime(a.Ctime, a.Ctimensec),
		}
		switch a.Typ {
		case meta.TypeFile:
			r.Size = a.Length
			st.bytes.Add(int64(a.Length))
		case meta.TypeDirectory:
			subdirs = append(subdirs, item{e.Inode, it.Depth + 1, p})
		case meta.TypeSymlink:
			var target []byte
			if eno := m.ReadLink(meta.Background(), e.Inode, &target); eno == 0 {
				s := string(target)
				r.Symlink = &s
				r.Size = uint64(len(target))
			}
		}
		b, err := json.Marshal(&r)
		if err != nil {
			return err
		}
		select {
		case out <- append(b, '\n'):
		case <-ctx.Done():
			return nil
		}
		st.records.Add(1)
	}
	st.dirs.Add(1)
	return q.Push(ctx, subdirs)
}
