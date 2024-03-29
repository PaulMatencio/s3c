package api

import (
	"context"
	"time"
)

func SetContext(timeout time.Duration)  (context.Context, context.CancelFunc) {

	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, timeout*time.Second)
	// defer cancel()
	return ctx,cancel
}