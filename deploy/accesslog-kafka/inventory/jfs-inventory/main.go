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

// jfs-inventory lists every path of a JuiceFS volume straight from the metadata engine,
// optionally spread over many workers on different machines.
//
// Standalone (one process, -threads goroutines):
//
//	jfs-inventory -meta redis://10.0.0.1:6379/1 -o inventory.ndjson.gz
//
// Distributed (run the same command on N machines; they share a directory queue in Redis
// and each writes its own part, the union of all parts is the inventory):
//
//	jfs-inventory -meta tikv://pd:2379/jfs -queue redis://coord:6379/0 -job 20261025 \
//	    -worker $HOSTNAME -o part-$HOSTNAME.ndjson.gz
//
// Workers pop a directory, list it with Readdir (attributes included) and push its
// sub-directories back, so the load balances itself however skewed the tree is. The
// metadata engine is usually the bottleneck; more workers only help while it has headroom.
package main

import (
	"compress/gzip"
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/juicedata/juicefs/pkg/meta"
	"github.com/juicedata/juicefs/pkg/utils"
	"github.com/sirupsen/logrus"
)

func main() {
	var (
		metaURL  = flag.String("meta", os.Getenv("META_URL"), "metadata engine URL (env META_URL); put the password in META_PASSWORD")
		queueURL = flag.String("queue", os.Getenv("QUEUE_URL"), "Redis URL of the shared directory queue (env QUEUE_URL); empty = standalone")
		job      = flag.String("job", os.Getenv("JOB_ID"), "job id shared by all workers of one run (env JOB_ID), required with -queue")
		wid      = flag.String("worker", os.Getenv("WORKER_ID"), "worker id used in logs (env WORKER_ID)")
		output   = flag.String("o", "-", "output file, .gz is compressed; - for stdout")
		volume   = flag.String("volume", "", "volume name stored in records (default: the name in the volume format)")
		subdir   = flag.String("subdir", "", "only list this sub-directory")
		snapshot = flag.String("snapshot", "", "snapshot time stored in records (default: now, UTC)")
		threads  = flag.Int("threads", 16, "concurrent directory listings in this worker")
		stall    = flag.Duration("stall-timeout", 10*time.Minute, "fail if the whole job makes no progress for this long")
		interval = flag.Duration("log-interval", 10*time.Second, "progress log interval")
		waitN    = flag.Int64("wait-workers", envInt("WAIT_WORKERS"), "with -queue, wait until this many workers joined before starting (env WAIT_WORKERS)")
		waitT    = flag.Duration("wait-timeout", 5*time.Minute, "maximum time to wait for -wait-workers")
	)
	flag.Parse()
	log.SetFlags(0) // pkg/utils already prefixes the standard logger with a timestamp
	utils.SetLogLevel(logrus.WarnLevel)
	if *metaURL == "" {
		log.Fatal("-meta is required")
	}
	if *queueURL != "" && *job == "" {
		log.Fatal("-job is required with -queue")
	}
	if *wid == "" {
		*wid, _ = os.Hostname()
	}
	if *snapshot == "" {
		*snapshot = time.Now().UTC().Format("2006-01-02T15:04:05+00:00")
	}

	conf := meta.DefaultConf()
	conf.ReadOnly = true
	conf.NoBGJob = true
	conf.Subdir = *subdir
	m := meta.NewClient(*metaURL, conf)
	format, err := m.Load(true)
	if err != nil {
		log.Fatalf("load volume: %s", err)
	}
	if *subdir != "" {
		if eno := m.Chroot(meta.Background(), *subdir); eno != 0 {
			log.Fatalf("chroot %s: %s", *subdir, eno)
		}
	}
	if *volume == "" {
		*volume = format.Name
	}

	var q workQueue = newLocalQueue()
	if *queueURL != "" {
		rq, err := newRedisQueue(*queueURL, *job)
		if err != nil {
			log.Fatalf("queue: %s", err)
		}
		if *waitN > 0 {
			n, err := rq.WaitWorkers(context.Background(), *waitN, *waitT)
			if err != nil {
				log.Fatalf("wait for workers: %s", err)
			}
			log.Printf("worker %s: %d/%d workers joined job %s", *wid, n, *waitN, *job)
		}
		q = rq
	}

	w, closeOut, err := createOutput(*output)
	if err != nil {
		log.Fatal(err)
	}
	start := time.Now()
	root := "/" + strings.Trim(*subdir, "/")
	st, err := run(context.Background(), m, q, w, options{
		Volume: *volume, Snapshot: *snapshot, RootPath: root, Threads: *threads,
		PopTimeout: time.Second, StallTimeout: *stall, LogInterval: *interval, Worker: *wid,
	})
	if cerr := closeOut(err == nil); err == nil {
		err = cerr
	}
	if err != nil {
		log.Fatalf("worker %s failed: %s", *wid, err)
	}
	log.Printf("worker %s finished in %s: %d directories, %d records, %d bytes in regular files, output %s",
		*wid, time.Since(start).Round(time.Millisecond), st.dirs.Load(), st.records.Load(), st.bytes.Load(), *output)
}

func envInt(name string) int64 {
	n, _ := strconv.ParseInt(os.Getenv(name), 10, 64)
	return n
}

// createOutput writes to <path>.tmp and renames it on success.
func createOutput(path string) (io.Writer, func(ok bool) error, error) {
	if path == "-" {
		return os.Stdout, func(bool) error { return nil }, nil
	}
	tmp := path + ".tmp"
	f, err := os.Create(tmp)
	if err != nil {
		return nil, nil, err
	}
	var w io.Writer = f
	var zw *gzip.Writer
	if strings.HasSuffix(path, ".gz") {
		zw, _ = gzip.NewWriterLevel(f, gzip.BestSpeed)
		w = zw
	}
	return w, func(ok bool) error {
		var err error
		if zw != nil {
			err = zw.Close()
		}
		if cerr := f.Close(); err == nil {
			err = cerr
		}
		if !ok || err != nil {
			_ = os.Remove(tmp)
			if err == nil {
				err = fmt.Errorf("aborted")
			}
			return err
		}
		return os.Rename(tmp, path)
	}, nil
}
