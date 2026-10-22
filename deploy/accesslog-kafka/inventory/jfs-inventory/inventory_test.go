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
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/juicedata/juicefs/pkg/meta"
)

// buildTree creates a skewed tree: one directory with many files, a deep chain, and
// special entries. It returns the expected "path type size" lines.
func buildTree(t *testing.T) (meta.Meta, []string) {
	m := meta.NewClient("memkv://", nil)
	if err := m.Init(&meta.Format{Name: "inv-test", DirStats: true}, true); err != nil {
		t.Fatalf("init: %s", err)
	}
	ctx := meta.Background()
	var want []string
	mkdir := func(parent meta.Ino, name, path string) meta.Ino {
		var ino meta.Ino
		if eno := m.Mkdir(ctx, parent, name, 0755, 0, 0, &ino, nil); eno != 0 {
			t.Fatalf("mkdir %s: %s", path, eno)
		}
		want = append(want, path+" directory 0")
		return ino
	}
	create := func(parent meta.Ino, name, path string) meta.Ino {
		var ino meta.Ino
		if eno := m.Create(ctx, parent, name, 0644, 0, 0, &ino, nil); eno != 0 {
			t.Fatalf("create %s: %s", path, eno)
		}
		want = append(want, path+" regular 0")
		return ino
	}
	big := mkdir(meta.RootInode, "big", "/big")
	for i := 0; i < 500; i++ {
		create(big, fmt.Sprintf("f%03d", i), fmt.Sprintf("/big/f%03d", i))
	}
	parent, path := meta.Ino(meta.RootInode), ""
	for d := 0; d < 30; d++ {
		name := fmt.Sprintf("d%d", d)
		path += "/" + name
		parent = mkdir(parent, name, path)
		create(parent, "leaf", path+"/leaf")
	}
	odd := mkdir(meta.RootInode, "odd entries", "/odd entries")
	f := create(odd, "报告,v1 \"q\" %20.txt", "/odd entries/报告,v1 \"q\" %20.txt")
	if eno := m.Link(ctx, f, big, "hardlink", nil); eno != 0 {
		t.Fatalf("link: %s", eno)
	}
	want = append(want, "/big/hardlink regular 0")
	if eno := m.Symlink(ctx, odd, "lnk", "../big/f001", nil, nil); eno != 0 {
		t.Fatalf("symlink: %s", eno)
	}
	want = append(want, "/odd entries/lnk symlink 11")
	sort.Strings(want)
	return m, want
}

func collect(t *testing.T, outs ...*bytes.Buffer) []string {
	var got []string
	for _, b := range outs {
		s := bufio.NewScanner(b)
		for s.Scan() {
			var r record
			if err := json.Unmarshal(s.Bytes(), &r); err != nil {
				t.Fatalf("bad record %q: %s", s.Text(), err)
			}
			got = append(got, fmt.Sprintf("%s %s %d", r.Path, r.Type, r.Size))
		}
	}
	sort.Strings(got)
	return got
}

func testOptions(worker string) options {
	return options{Volume: "inv-test", Snapshot: "s", RootPath: "/", Threads: 4, PopTimeout: 50 * time.Millisecond,
		StallTimeout: time.Minute, LogInterval: time.Hour, Worker: worker}
}

func checkSame(t *testing.T, got, want []string) {
	t.Helper()
	if strings.Join(got, "\n") != strings.Join(want, "\n") {
		t.Fatalf("inventory mismatch: got %d records, want %d", len(got), len(want))
	}
}

func TestStandalone(t *testing.T) {
	m, want := buildTree(t)
	var out bytes.Buffer
	st, err := run(context.Background(), m, newLocalQueue(), &out, testOptions("w0"))
	if err != nil {
		t.Fatal(err)
	}
	checkSame(t, collect(t, &out), want)
	if st.dirs.Load() != 33 {
		t.Fatalf("listed %d directories, want 33", st.dirs.Load())
	}
}

func runWorkers(t *testing.T, m meta.Meta, newQueue func() workQueue, n int) []*bytes.Buffer {
	outs := make([]*bytes.Buffer, n)
	errs := make([]error, n)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		outs[i] = &bytes.Buffer{}
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, errs[i] = run(context.Background(), m, newQueue(), outs[i], testOptions(fmt.Sprint(i)))
		}(i)
	}
	wg.Wait()
	for _, err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	return outs
}

// Workers sharing one queue must produce disjoint parts whose union is the inventory.
func TestSharedQueue(t *testing.T) {
	m, want := buildTree(t)
	q := newLocalQueue()
	outs := runWorkers(t, m, func() workQueue { return q }, 4)
	checkSame(t, collect(t, outs...), want)
}

func TestRedisQueue(t *testing.T) {
	url := os.Getenv("JFS_INVENTORY_TEST_REDIS")
	if url == "" {
		t.Skip("set JFS_INVENTORY_TEST_REDIS=redis://host:6379/15 to run")
	}
	m, want := buildTree(t)
	job := fmt.Sprintf("test-%d", time.Now().UnixNano())
	outs := runWorkers(t, m, func() workQueue {
		q, err := newRedisQueue(url, job)
		if err != nil {
			t.Fatal(err)
		}
		return q
	}, 3)
	checkSame(t, collect(t, outs...), want)
}

func TestRedisWaitWorkers(t *testing.T) {
	url := os.Getenv("JFS_INVENTORY_TEST_REDIS")
	if url == "" {
		t.Skip("set JFS_INVENTORY_TEST_REDIS=redis://host:6379/15 to run")
	}
	job := fmt.Sprintf("wait-%d", time.Now().UnixNano())
	var wg sync.WaitGroup
	got := make([]int64, 3)
	for i := range got {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			time.Sleep(time.Duration(i) * 300 * time.Millisecond) // staggered starts
			q, _ := newRedisQueue(url, job)
			got[i], _ = q.WaitWorkers(context.Background(), 3, 10*time.Second)
		}(i)
	}
	wg.Wait()
	for i, n := range got {
		if n != 3 {
			t.Fatalf("worker %d saw %d workers, want 3", i, n)
		}
	}
}

func TestStallDetection(t *testing.T) {
	m, _ := buildTree(t)
	q := newLocalQueue()
	// a dead worker took the root and never finished it
	if _, err := q.Seed(context.Background(), item{meta.RootInode, 0, ""}); err != nil {
		t.Fatal(err)
	}
	if _, ok, _ := q.Pop(context.Background(), time.Second); !ok {
		t.Fatal("expected the root item")
	}
	opt := testOptions("w1")
	opt.StallTimeout = 300 * time.Millisecond
	var out bytes.Buffer
	if _, err := run(context.Background(), m, q, &out, opt); err == nil || !strings.Contains(err.Error(), "no progress") {
		t.Fatalf("expected a stall error, got %v", err)
	}
}

func TestItemEncoding(t *testing.T) {
	it := item{42, 3, "/a/b\tc d"}
	got, err := decodeItem(it.encode())
	if err != nil || got != it {
		t.Fatalf("round trip: %+v %v", got, err)
	}
}
