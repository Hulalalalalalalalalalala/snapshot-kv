package main

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/Hulalalalalalalalalalala/snapshot-kv"
)

func TestStatsCommandOutput(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "store")

	s, err := snapshot.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Put("a", []byte("abcd")); err != nil {
		t.Fatal(err)
	}
	if err := s.Put("b", []byte("ef")); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Snapshot(); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Snapshot(); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	var out bytes.Buffer
	if code := run([]string{"--dir", dir, "stats"}, &out); code != 0 {
		t.Fatalf("exit code %d", code)
	}
	want := "{\"keys\":2,\"snapshots\":2,\"avgBytes\":3.00}\n"
	if out.String() != want {
		t.Fatalf("output=%q want=%q", out.String(), want)
	}
}

func TestStatsCommandEmpty(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "store")
	s, err := snapshot.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	s.Close()

	var out bytes.Buffer
	if code := run([]string{"--dir=" + dir, "stats"}, &out); code != 0 {
		t.Fatalf("exit code %d", code)
	}
	want := "{\"keys\":0,\"snapshots\":0,\"avgBytes\":0.00}\n"
	if out.String() != want {
		t.Fatalf("output=%q want=%q", out.String(), want)
	}
}

func TestStatsCommandMissingDirSilent(t *testing.T) {
	missing := filepath.Join(t.TempDir(), "does-not-exist")
	var out bytes.Buffer
	if code := run([]string{"--dir", missing, "stats"}, &out); code != 1 {
		t.Fatalf("exit code=%d, want 1", code)
	}
	if out.Len() != 0 {
		t.Fatalf("expected no output, got %q", out.String())
	}
}

func TestStatsCommandPathIsFileSilent(t *testing.T) {
	file := filepath.Join(t.TempDir(), "f")
	if err := os.WriteFile(file, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	if code := run([]string{"--dir", file, "stats"}, &out); code != 1 || out.Len() != 0 {
		t.Fatalf("code=%d out=%q", code, out.String())
	}
}

func TestStatsCommandBadArgs(t *testing.T) {
	for _, args := range [][]string{
		{"stats"},
		{"--dir", "/tmp"},
		{"--dir", "/tmp", "frobnicate"},
		{"--dir"},
	} {
		var out bytes.Buffer
		if code := run(args, &out); code != 1 {
			t.Fatalf("args=%v code=%d", args, code)
		}
		if out.Len() != 0 {
			t.Fatalf("args=%v produced output %q", args, out.String())
		}
	}
}

func TestStatsOutputIsCompactSingleLine(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "store")
	s, err := snapshot.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Put("k", []byte("v")); err != nil {
		t.Fatal(err)
	}
	s.Close()

	var out bytes.Buffer
	run([]string{"--dir", dir, "stats"}, &out)
	line := out.String()
	if !strings.HasSuffix(line, "}\n") || strings.Count(line, "\n") != 1 {
		t.Fatalf("not exactly one line: %q", line)
	}
	if strings.ContainsAny(line, " ") {
		t.Fatalf("contains spaces: %q", line)
	}
}
