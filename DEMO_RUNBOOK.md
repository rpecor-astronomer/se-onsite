# Scenario 3 — Legacy Scheduler Migration Demo

**Persona:** Director of Batch Operations, ~$40B regional bank
**Pain:** Control-M renewal + 18% licensing hike; CIO wants a modernization plan
**Fear #1:** 14 operators, none write Python
**Fear #2:** Losing Control-M's visual dependency view before a 6 AM reporting deadline
**Fear #3:** Compliance requires every job change tied to an approval — auditable

## The pitch (one sentence)

> Astro lets your operators keep authoring in a GUI-style view they already understand — YAML, not Python — while every change flows through a PR, every overnight run is graphed like Control-M, and every regulator-facing release stops on a named compliance officer's approval. That's the 18% renewal money back, plus audit and observability the current tool can't touch.

---

## What we're showing (three artifacts, one DAG)

1. **`dags/templates/batch_blueprints.py`** — Astronomer's open-source [Blueprint](https://github.com/astronomer/blueprint) library. SEs (or a platform team) write these *once* to expose Control-M-style primitives:
   `mainframe_job`, `distributed_job`, `file_watch`, `reconciliation`, `regulatory_report`.
2. **`dags/overnight_loan_servicing.dag.yaml`** — the file the *operator* touches. No Python. Reads like a Control-M job dependency table.
3. **`dags/loader.py`** — three lines. Every `*.dag.yaml` in the folder becomes a DAG. Operators never edit it.

---

## 10-minute demo script

### 0:00 — 1:00  Frame the fears
- 3,000 Control-M jobs, 14 operators, none write Python, 18% renewal hike.
- Three failed "just script it in Python" attempts.
- We're going to knock down all three fears in one demo.

### 1:00 — 3:00  Fear #1 — "No one writes Python"
- Open **`dags/overnight_loan_servicing.dag.yaml`**.
- Point out: this looks like a Control-M job table. `wait_for_gl_extract` → `post_daily_interest` → `amortization_recalc` → fan-out to `distribute_statements` + `core_recon` → converge on `call_report`.
- Change `poke_seconds: 30` → `60`. Save. That's the operator's day.
- Mention the **Astro IDE**: same YAML, but rendered as a visual builder with drop-downs (fed by the JSON schema Blueprint generates). Non-technical operators get form fields; technical ones stay in YAML. Nobody writes Python.

### 3:00 — 5:00  Fear #2 — "We'll lose the visual dependency view"
- Switch to Airflow UI → `overnight_loan_servicing` DAG → **Graph** view.
- Same picture Control-M shows, live. Green/yellow/red per task, click a node → logs, retries, XComs, upstream/downstream.
- Show **Grid** view: 30 days of history at a glance — Control-M can't do this without a bolt-on.
- Mention **Astro Observe / Alerts**: SLA on the 6 AM deadline; a scheduler-level alert fires *before* a human wakes up, not after the regulator calls.

### 5:00 — 7:30  Fear #3 — "Compliance needs every change tied to an approval"
Two auditability layers — both live:

**a) Change-time audit (Git + Astro deploys)**
- Show the YAML in a PR. Reviewer sees exactly which line moved.
- `astro deploy` pushes only the reviewed image. The deployment record ties the code hash to who approved the PR. That's SOX change-control evidence, free.

**b) Run-time audit (HITL approval on the regulator release)**
- Trigger `overnight_loan_servicing`.
- Watch it run down to the `call_report.approve_release` step.
- Airflow UI → **Browse → Required Actions** → the run is paused waiting for a compliance officer.
- Approve with a comment ("Reviewed CALL report v2 — release").
- The response is recorded against the run, the user, and the timestamp. Immutable. Reject → downstream skips, run holds.

### 7:30 — 9:00  The business case (2 slides worth, spoken)
- **Replace** the 18% renewal. Save the license line.
- **Reuse** existing operator muscle memory — job chains stay job chains.
- **Add** capability Control-M can't ship: version-controlled changes, HITL compliance gates, an OpenLineage graph across the whole batch estate, alerting that beats the deadline.
- **Reduce risk** on the three failed "port to Python" attempts: this isn't a port to Python; it's a port to *YAML that a platform team maintains once*.

### 9:00 — 10:00  Close
- Phase 1 (30 days): 5 highest-touch Control-M folders migrated to YAML — running side-by-side with Control-M.
- Phase 2 (90 days): regulatory reports moved behind HITL. Compliance signs the runbook.
- Phase 3 (180 days): Control-M renewal not signed.

---

## Judge-question backstops

- **"What if the DAG author needs something the blueprint doesn't cover?"**
  Blueprints are Python — the platform team adds a new one in an afternoon. And Airflow DAGs are open Python underneath, so escape hatches always exist. Operators never see it.
- **"How does this scale to 3,000 jobs?"**
  YAML per DAG, one loader, one image. Blueprint's `Builder` API can also generate DAGs in a loop from a table — one row per Control-M folder.
- **"What about mainframe SSH / z/OS connectivity?"**
  Airflow's SSH + IBM providers already ship on Astro Runtime; the demo uses `BashOperator` for optics, prod would swap in `SSHOperator` or the z/OS provider without touching the YAML.
- **"Why not just use dag-factory?"**
  Blueprint gives Pydantic-validated configs, JSON Schema for the Astro IDE, and versioned templates — safer for a bank than free-form YAML.

---

## One-line project verification (in case a judge asks)

```
astro dev parse   # ✔ No errors detected in your DAGs
```
