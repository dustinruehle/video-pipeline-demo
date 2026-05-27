# BUILD_LOG.md — Video Pipeline Demo

Wall-clock budget: 4 hours. Stop adding scope at 3:30. Stop entirely at 4:00.

- [23:03] phase=start — main agent reads specs, dispatches deck subagent, begins upstream inspection
- [23:05] phase=inspect — INSPECTION.md and NOTICES.md written; upstream commit 51f45040
- [23:14] phase=patternv-green — Pattern V: types, searchattrs, tclient, planexec, activities, workflow, worker, starter all built and tested. Lint clean. Deck subagent reported complete at 23:11.
- [23:17] phase=patternd-green — Pattern D dsl/workflow/worker-b/starter-b + tests in. Three dsl/*.yaml files. make build passes.
- [23:18] phase=integration — Histories captured. All unit + replay tests pass against captured histories.
- [23:20] phase=demo-end-to-end — ./demo-a.sh and ./demo-b.sh both run end-to-end against dev server. All narration moments printed; replay sed toggle pass→fail→restore confirmed working.
- [23:22] phase=cold-rehearsal — `make teardown && make setup && ./demo-a.sh && ./demo-b.sh` from clean. Setup 4s, demo-a 106s, demo-b 52s. TOTAL 162s (2:42).
- [23:25] phase=wrap — vendor-reference/ removed, README + BUILD_REPORT written, final test + lint sweep green.
