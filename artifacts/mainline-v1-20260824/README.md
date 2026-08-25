# Mainline v1 deletion baseline

- Captured before migration `042_rebuild_mainline_performance_schema.sql` on 2026-08-24.
- `summary.csv` contains PostgreSQL planner row estimates and TimescaleDB chunk counts.
- `sample-latest.csv` contains 100 deterministic stock rows for 2026-08-21.
- `sample-history.csv` contains the available SW2021 L2 industry rows for 2019-01-04.
- The files contain derived-factor validation samples only. Raw market, financial,
  fund, ETF, index-weight and SW classification tables were not deleted.

The baseline is intended for schema/type and sampled numerical regression. It is
not a full v1 backup; full v1 derived rows were explicitly approved for deletion.
