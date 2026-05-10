# zqlz-table-workflows

Ownership boundary for table workflow orchestration that is independent of GPUI and app-shell concerns.

## Owns

- typed request/response contracts for table workflow decisions
- validation and guardrails for table workflow inputs
- deterministic planning outputs consumed by app adapters

## Does not own

- direct GPUI window/dock/focus interaction
- notification rendering or modal presentation
- low-level database execution

## Migration role

This crate is introduced as a strangler target for logic currently in:

- `crates/zqlz-app/src/main_view/table_handlers/table_ops/**`

The app currently calls these APIs via temporary adapters in `main_view`.
