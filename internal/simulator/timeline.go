package simulator

import (
	"context"
	"io"
	"log/slog"
	"time"
)

// epoch is the wall-clock instant that stands for simulated time 0. Records
// carry fake time as epoch+Now() so that the standard TextHandler can format
// it through ReplaceAttr; nothing ever reads wall-clock time.
var epoch = time.Unix(0, 0).UTC()

// newTimelineLogger returns a logger whose output is the simulation
// timeline: one line per record in the form
//
//	t=<ms> event=<message> key=value ...
//
// t is the fake clock in milliseconds at the moment of the call; level and
// wall-clock time are omitted so that two runs with the same seed produce
// byte-identical output. Every level is enabled.
func newTimelineLogger(clock *Clock, w io.Writer) *slog.Logger {
	text := slog.NewTextHandler(w, &slog.HandlerOptions{
		Level: slog.LevelDebug,
		ReplaceAttr: func(groups []string, a slog.Attr) slog.Attr {
			if len(groups) != 0 {
				return a
			}
			switch a.Key {
			case slog.TimeKey:
				return slog.Int64("t", a.Value.Time().Sub(epoch).Milliseconds())
			case slog.LevelKey:
				return slog.Attr{} // dropped
			case slog.MessageKey:
				a.Key = "event"
			}
			return a
		},
	})
	return slog.New(&timelineHandler{clock: clock, inner: text})
}

// timelineHandler stamps each record with the fake clock before handing it
// to the TextHandler, which turns the stamp into the leading t= attribute.
type timelineHandler struct {
	clock *Clock
	inner slog.Handler
}

func (h *timelineHandler) Enabled(ctx context.Context, level slog.Level) bool {
	return h.inner.Enabled(ctx, level)
}

func (h *timelineHandler) Handle(ctx context.Context, r slog.Record) error {
	r.Time = epoch.Add(h.clock.Now())
	return h.inner.Handle(ctx, r)
}

func (h *timelineHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return &timelineHandler{clock: h.clock, inner: h.inner.WithAttrs(attrs)}
}

func (h *timelineHandler) WithGroup(name string) slog.Handler {
	return &timelineHandler{clock: h.clock, inner: h.inner.WithGroup(name)}
}
