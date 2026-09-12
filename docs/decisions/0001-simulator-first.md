# ADR 0001: Simulator-first development

## Status

Accepted.

## Context

Raft bugs are timing and ordering bugs: split votes, delayed replies from a
previous term, duplicated AppendEntries arriving after a truncation. Tests
built on real goroutines, sockets and `time.Sleep` cannot reproduce such
interleavings on demand and become flaky as soon as they try.

## Decision

All protocol behavior is developed and tested against a deterministic,
single-threaded simulator *before* any real networking exists. The simulator
owns a fake clock, an ordered event queue and a simulated network with fault
injection, and it drives the Raft core exactly the way the production runtime
will. Randomness is seeded and the seed is logged, so any failure replays.

Real networking (`net/http`) is added last, as a second host for the same core.

## Consequences

- The core must be host-agnostic (see ADR 0002).
- Tests use `clock.Advance(...)` and explicit message delivery, never sleeps.
- Safety invariants can be asserted after every simulated event.
- Wall-clock tests exist only for the real transport and are bounded and
  skippable with `-short`.
