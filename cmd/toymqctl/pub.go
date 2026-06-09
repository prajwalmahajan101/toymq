package main

import (
	"context"
	"io"
)

func runPub(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	_ = ctx
	_ = args
	_ = stdout
	_ = stderr
	return exitOK
}
