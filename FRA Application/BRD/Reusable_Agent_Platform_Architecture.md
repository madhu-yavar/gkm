# Reusable Agent Platform Architecture

## Purpose
This document captures the target architecture for building a reusable agentic analytics and review platform.

The goal is not to build one-off dashboards per use case. The goal is to build a common platform with:
- platform-dependent agents
- domain-dependent agents
- canonical bundles as the single source of truth
- reusable dashboard, chat, and summary surfaces
- human-in-the-loop controls

This reference is intended to guide future design, engineering, and roadmap discussions.

## Core Product Understanding
The platform is meant to support workflows like:
- Excel or structured-data uploads
- extraction and normalization
- comparison based on business logic
- insights based on user checklists
- anomaly detection
- forecasting
- summary/report generation using user-defined templates
- dashboard generation and refinement
- chat over the resulting analytical bundle

This overall pattern is reusable across domains.

Examples:
- financial statement review
- dues and collections review
- operational status review
- staffing/productivity review
- catalog/master-data review

What changes by domain is not the outer platform flow, but:
- entities
- metrics
- rules
- anomaly definitions
- comparisons
- severity logic
- templates

## Design Principle
The system must be split into two layers:

1. Platform layer
- reusable across all domains
- owns ingestion, orchestration, bundles, logging, HITL, dashboards, reports, and chat

2. Domain layer
- specific to business context
- owns semantics, rules, thresholds, comparisons, anomaly logic, forecast logic, and templates

The dashboard agent must not contain hardcoded domain intelligence.
It should consume structured outputs from platform and domain agents.

## High-Level Architecture

```text
User / Reviewer
  -> Upload files
  -> Provide checklist / context / template
  -> Review findings
  -> Refine widgets and summaries
  -> Resolve / sign off

              |
              v

      Orchestrator Agent
      - workflow control
      - state transitions
      - routing between agents
      - HITL checkpoints

              |
      -------------------------
      |                       |
      v                       v
Platform Agent Pool     Domain Agent Pool
```

## Platform Agent Pool
These agents are reusable across all supported domains.

### 1. Intake Agent
Responsibilities:
- upload/session management
- input registration
- user intent capture
- checklist capture
- summary-template capture
- processing kickoff

Inputs:
- files
- user instructions
- checklist
- summary template

Outputs:
- source registration record
- intake context

### 2. Extraction Agent
Responsibilities:
- parse Excel, CSV, PDF, API exports
- detect sheets, tables, sections
- detect structural boundaries
- infer raw field types
- normalize raw extracted content

Outputs:
- canonical raw source model

### 3. Semantic Agent
Responsibilities:
- infer entities, dimensions, measures, periods
- infer business meaning from structure and examples
- normalize labels for downstream use
- identify ambiguities and follow-up questions

Outputs:
- semantic bundle

### 4. Alignment Agent
Responsibilities:
- compare multiple processed documents
- suggest document similarity
- infer join keys
- infer period/version ordering
- prepare merge plan for combined analysis

Outputs:
- alignment bundle
- merge proposal

### 5. EDA Agent
Responsibilities:
- profiling
- missingness scan
- duplicates scan
- outlier scan
- concentration analysis
- distribution analysis
- trend readiness detection
- baseline descriptive analytics

Outputs:
- EDA bundle

### 6. Forecast Agent
Responsibilities:
- detect time-series suitability
- forecast readiness gating
- deterministic/statistical forecasting
- confidence and limitation scoring

Outputs:
- forecast bundle

### 7. Insight Agent
Responsibilities:
- convert verified outputs into ranked findings
- score findings by:
  - materiality
  - confidence
  - actionability
  - stakeholder relevance

Outputs:
- insight bundle

### 8. Dashboard Agent
Responsibilities:
- generate dashboard schema
- propose tabs, widgets, controls, drilldowns
- refine layout from user input
- map findings into runtime widgets

Outputs:
- dashboard bundle

### 9. Summary Agent
Responsibilities:
- generate executive summary
- generate analytics summary
- map output into user-defined templates
- keep business-facing and analytics-facing outputs separate

Outputs:
- summary bundle

### 10. Chat Agent
Responsibilities:
- answer only from canonical bundle evidence
- avoid recomputation drift
- explain findings, trends, and anomalies
- support stakeholder questions

Outputs:
- grounded chat responses

### 11. HITL Agent
Responsibilities:
- collect user confirmations
- capture answers to business questions
- confirm joins/period ordering
- manage reviewer dispositions
- coordinate approval workflows

Outputs:
- confirmed decisions
- resolution status

### 12. Audit / Log Agent
Responsibilities:
- capture state transitions
- capture workflow traces
- capture agent/tool/model context
- preserve lineage of bundles and approvals

Outputs:
- audit/log records

## Domain Agent Pool
These agents vary by business domain.

### A. Domain Semantic Agent
Responsibilities:
- domain vocabulary
- entity model
- expected measures
- expected relationships
- business definitions

Examples:
- financial statements: GL account, vendor, line item, related party
- collections: tower, unit, owner, dues, penalty
- operations: return, preparer, reviewer, status, SLA

### B. Domain Rule Agent
Responsibilities:
- deterministic business checks
- thresholds
- validation policies
- completeness logic

Examples:
- account type mismatch
- overdue payment rule
- missing payroll tax account
- status aging breach

### C. Domain Comparison Agent
Responsibilities:
- domain-specific comparison logic
- prior-period comparisons
- benchmark comparisons
- peer or cohort comparisons

Examples:
- YoY financial variance
- quarter-wise tower dues movement
- staff throughput comparison

### D. Domain Anomaly Agent
Responsibilities:
- define anomalous patterns for the domain
- suppress false positives
- attach business rationale

Examples:
- unusual vendor-to-account posting
- sudden tower-level penalty spike
- ratio deviation from industry norms

### E. Domain Forecast Agent
Responsibilities:
- domain-specific forecast logic
- define what is forecastable
- attach model constraints and interpretation

Examples:
- next-period cash trend
- collections forecast by tower
- operational throughput forecast

### F. Domain Insight Agent
Responsibilities:
- phrase findings in domain language
- recommend domain-specific actions
- rank findings by domain materiality

### G. Domain Template Agent
Responsibilities:
- support domain-specific report structure
- support user-defined summary templates
- shape dashboard/report output for the domain

## End-to-End Flow

```text
User Input / Files / Checklist / Template
  -> Intake Agent
  -> Extraction Agent
  -> Semantic Agent
  -> Domain Semantic Agent
  -> Alignment Agent (if multi-document)
  -> EDA Agent
  -> Domain Rule Agent
  -> Domain Comparison Agent
  -> Domain Anomaly Agent
  -> Forecast Agent
  -> Domain Forecast Agent
  -> Insight Agent
  -> Domain Insight Agent
  -> Dashboard Agent
  -> Summary Agent
  -> Chat Agent
  -> HITL / Reviewer Resolution / Sign-Off
```

## Canonical Bundles
The platform should persist versioned, canonical bundles.
These should be the only truth source for dashboards, chat, and summaries.

### Recommended Platform Bundles
- `SourceBundle`
- `SemanticBundle`
- `AlignmentBundle`
- `EDABundle`
- `ForecastBundle`
- `InsightBundle`
- `DashboardBundle`
- `SummaryBundle`
- `ReviewBundle` or `DecisionBundle`

### Bundle Design Principles
- versioned
- persisted
- auditable
- render-safe
- user-facing systems read from bundles only
- no silent recomputation in individual surfaces

## Platform vs Domain Responsibilities

### Platform Owns
- uploads
- extraction
- orchestration
- canonical bundles
- logs and audit
- HITL mechanics
- dashboard/report/chat rendering
- template routing
- multi-document alignment infrastructure

### Domain Owns
- entity definitions
- metric logic
- rule libraries
- anomaly logic
- comparison logic
- forecast logic
- severity logic
- recommendation logic
- final domain-specific presentation details

## Example Domain Mapping

### 1. Financial Statement Review
Platform reusables:
- upload
- GL/FS extraction
- dashboard/report/chat
- HITL
- bundles

Domain specifics:
- chart of accounts semantics
- account coding rules
- ratio analysis
- source-document reconciliation logic
- finding severity and resolution workflow

### 2. Collections / Dues
Platform reusables:
- workbook extraction
- dashboard generation
- anomaly/trend/forecast infrastructure
- summaries

Domain specifics:
- tower/unit/entity mapping
- dues/penalty measures
- overdue logic
- quarter-wise movement comparisons

### 3. Operational Workflow / Tax Return Status
Platform reusables:
- extraction
- dashboard/report/chat
- bundle persistence

Domain specifics:
- queue states
- SLA logic
- reviewer/preparer routing
- stale-case detection

## Why This Architecture Matters
This architecture prevents the system from becoming a collection of one-off dashboards.

It enables:
- reuse of platform capabilities across domains
- pluggable domain intelligence
- better maintainability
- clearer ownership boundaries
- lower drift between dashboard, chat, and summaries
- easier auditability and governance

## Recommended Build Direction
The current dashboard-agent system should be evolved into:
- a reusable platform shell
- plus domain modules/agents

The dashboard agent should become a presentation/orchestration agent, not the source of domain intelligence.

Recommended sequencing:

1. Stabilize canonical bundle architecture
2. Separate platform agents from domain agents
3. Build one domain deeply end to end
4. Generalize domain plugin interfaces
5. Add more domains without rewriting the platform shell

## Future Technical Blueprint
This document is conceptual.
The next design artifact should define:
- services/modules
- DB tables
- bundle schemas
- agent graph
- API endpoints
- HITL workflow states
- MVP scope for the first target domain

## One-Line Product Definition
This platform is best understood as:

**A reusable agentic analytics and review operating system with a common platform shell and pluggable domain intelligence.**
