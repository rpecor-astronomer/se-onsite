"""Blueprint templates that mirror Control-M primitives.

An operator authors YAML — no Python — and Blueprint composes these
templates into an Airflow DAG. Each class maps to a Control-M concept
the batch ops team already thinks in:

    MainframeJob         -> JCL / z/OS batch step
    DistributedJob       -> Unix shell script step
    FileWatch            -> Control-M file trigger
    Reconciliation       -> Core banking recon step (fail-loud)
    RegulatoryReport     -> SOX / call report step, gated by HITL approval
"""

from __future__ import annotations

from typing import Any, Literal

from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.hitl import ApprovalOperator
from airflow.providers.standard.sensors.filesystem import FileSensor
from airflow.sdk import Param, TaskGroup
from pydantic import ConfigDict
from blueprint import BaseModel, Blueprint, BlueprintDagArgs, Field


# ---------------------------------------------------------------------------
# DAG-level YAML (tags, owner, retries) — one subclass per project.
# ---------------------------------------------------------------------------
class BatchDagArgsConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")
    schedule: str | None = None
    description: str | None = None
    tags: list[str] = Field(default_factory=list)
    owner: str = "batch-ops"
    retries: int = 1


class BatchDagArgs(BlueprintDagArgs[BatchDagArgsConfig]):
    def render(self, config: BatchDagArgsConfig) -> dict[str, Any]:
        return {
            "schedule": config.schedule,
            "description": config.description,
            "tags": config.tags + ["control-m-migration"],
            "default_args": {"owner": config.owner, "retries": config.retries},
        }


# ---------------------------------------------------------------------------
# Mainframe / distributed job primitives.
# ---------------------------------------------------------------------------
class MainframeJobConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")
    jcl_member: str = Field(description="JCL member name (e.g. LNSVC010)")
    lpar: Literal["PROD1", "PROD2", "DR"] = Field(
        default="PROD1", description="Target LPAR"
    )
    max_rc: int = Field(default=4, ge=0, le=16, description="Highest acceptable RC")


class MainframeJob(Blueprint[MainframeJobConfig]):
    """Submit a JCL member to z/OS and wait for completion."""

    def render(self, config: MainframeJobConfig) -> BashOperator:
        return BashOperator(
            task_id=self.step_id,
            bash_command=(
                f"echo '[MF {config.lpar}] SUBMIT {config.jcl_member} "
                f"(max RC {config.max_rc})' && sleep 1"
            ),
        )


class DistributedJobConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")
    script: str = Field(description="Absolute path to the shell script")
    host_group: str = Field(default="unix-batch", description="Control-M host group")


class DistributedJob(Blueprint[DistributedJobConfig]):
    """Run a Unix batch script on a distributed host group."""

    def render(self, config: DistributedJobConfig) -> BashOperator:
        return BashOperator(
            task_id=self.step_id,
            bash_command=(
                f"echo '[{config.host_group}] exec {config.script}' && sleep 1"
            ),
        )


# ---------------------------------------------------------------------------
# File trigger — Control-M's most-used primitive.
# ---------------------------------------------------------------------------
class FileWatchConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")
    path: str = Field(description="Path or glob to watch")
    poke_seconds: int = Field(default=30, ge=5)
    timeout_minutes: int = Field(default=60, ge=1)


class FileWatch(Blueprint[FileWatchConfig]):
    """Wait for an upstream file — the Control-M file-trigger equivalent."""

    def render(self, config: FileWatchConfig) -> FileSensor:
        return FileSensor(
            task_id=self.step_id,
            filepath=config.path,
            poke_interval=config.poke_seconds,
            timeout=config.timeout_minutes * 60,
            mode="reschedule",
        )


# ---------------------------------------------------------------------------
# Core banking reconciliation.
# ---------------------------------------------------------------------------
class ReconciliationConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")
    system_a: str = Field(description="Source-of-truth system")
    system_b: str = Field(description="System to reconcile against A")
    tolerance_cents: int = Field(default=0, ge=0)


class Reconciliation(Blueprint[ReconciliationConfig]):
    """Two-way reconciliation between core banking systems."""

    def render(self, config: ReconciliationConfig) -> BashOperator:
        return BashOperator(
            task_id=self.step_id,
            bash_command=(
                f"echo 'RECON {config.system_a} <-> {config.system_b} "
                f"(tolerance {config.tolerance_cents}c)' && sleep 1"
            ),
        )


# ---------------------------------------------------------------------------
# Regulatory report with a compliance-gated release step.
# ---------------------------------------------------------------------------
class RegulatoryReportConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")
    report_code: Literal["CALL", "SOX-404", "FFIEC-031", "FR-Y-9C"] = Field(
        description="Regulatory report identifier"
    )
    prepared_by: str = Field(default="regops", description="Preparer team")
    approval_timeout_minutes: int = Field(default=45, ge=1)


class RegulatoryReport(Blueprint[RegulatoryReportConfig]):
    """Generate a regulatory report and hold release for compliance sign-off.

    The `approve_release` step is an Airflow 3 HITL ApprovalOperator: the run
    pauses in the UI under Browse > Required Actions until a named compliance
    officer approves or rejects. Every response is auditable and tied to a
    specific run — this is the auditability the bank's Compliance team needs.
    """

    def render(self, config: RegulatoryReportConfig) -> TaskGroup:
        with TaskGroup(group_id=self.step_id) as group:
            generate = BashOperator(
                task_id="generate",
                bash_command=(
                    f"echo 'Preparing {config.report_code} "
                    f"(by {config.prepared_by})' && sleep 1"
                ),
            )

            approve = ApprovalOperator(
                task_id="approve_release",
                subject=f"Release {config.report_code}?",
                body=(
                    f"**Report:** {config.report_code}\n\n"
                    f"**Prepared by:** {config.prepared_by}\n\n"
                    "Review the generated artifact and approve to release "
                    "to the regulator, or reject to hold."
                ),
                execution_timeout=None,
                params={"comments": Param("", type="string")},
            )

            release = BashOperator(
                task_id="release_to_regulator",
                bash_command=(
                    f"echo 'RELEASED {config.report_code} to regulator'"
                ),
            )

            generate >> approve >> release
        return group
