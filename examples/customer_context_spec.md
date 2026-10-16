# Subscription Analytics — Domain Specification

This document describes the business meaning of the subscription-analytics data
estate. It is intended as uploaded customer context: a reviewer attaches it to a
schema scope (for example `main.subscriptions`) so generated table and column
descriptions reflect how the business actually uses these terms. Because this
specification is longer than a few hundred words, the upload path will summarize
it into a concise business-context entry rather than storing it verbatim.

## Business overview

The subscription-analytics estate tracks the full lifecycle of a customer's
relationship with our software-as-a-service product: how they sign up, which plan
they are on, how their plan changes over time, how much they are billed, and when
and why they leave. The data supports three primary consumers: finance (revenue
recognition and forecasting), growth (acquisition and conversion funnels), and
customer success (retention and churn-risk work). Each consumer tends to read the
same tables through a different lens, so descriptions should favor neutral,
business-level language rather than team-specific jargon.

## Key terms and conventions

- **Account** is the billable organization, not an individual person. One account
  may contain many **users** (seats). Metrics such as revenue are measured per
  account; engagement metrics are usually measured per user.
- **Subscription** is a single account's active commitment to a plan for a period.
  An account can have at most one active subscription at a time, but its history
  may contain many closed subscriptions after upgrades, downgrades, or churn.
- **MRR** (monthly recurring revenue) is normalized to a monthly figure even when
  the account is billed annually; divide annual contract value by twelve. **ARR**
  is simply MRR multiplied by twelve. Both exclude one-time charges and taxes.
- **Churn** means an account's subscription ended and was not replaced within the
  same billing cycle. A downgrade is not churn. A failed payment that later
  recovers (dunning) is not churn until the recovery window closes.
- All monetary columns are in USD and stored in cents to avoid floating-point
  rounding; divide by one hundred for display. All timestamps are UTC.

## Table conventions

Tables prefixed `fct_` are fact tables at event grain: one row per billing event,
plan change, or lifecycle transition. Tables prefixed `dim_` are slowly changing
dimensions; the current row is flagged with `is_current = true`, and historical
rows carry `valid_from` and `valid_to` timestamps. Columns ending in `_id` are
surrogate keys that join to the matching `dim_` table, not natural identifiers.
Columns ending in `_amt` are monetary measures (in cents), and columns ending in
`_at` are event timestamps.

## Status and category codes

The `subscription_status` column uses: `trialing`, `active`, `past_due`,
`canceled`, and `paused`. The `plan_tier` column uses: `free`, `starter`,
`pro`, and `enterprise`. The `churn_reason` column is free text entered by
customer success and should be treated as a qualitative note, not a controlled
vocabulary.

## Data quality notes

Do not treat `email_domain` as personally identifying; the raw email address is
never stored in the analytics estate, only the hashed domain for cohort analysis.
Early-period rows (before the platform migration) may have null `plan_tier`;
these predate tiered pricing and should be described as legacy accounts rather
than data errors. The `mrr_amt` column can briefly go negative during a refund
event; this is expected and nets out within the same billing cycle.
