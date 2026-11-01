// Past incidents shown on the Pipeline page, newest first. Written by hand
// after each one is resolved (a short public postmortem).
export const INCIDENTS = [
  {
    title: '12 nights without a batch run',
    resolved: '2026-11-01',
    events: [
      {
        when: 'Oct 19 · 17:28 UTC',
        tone: 'down',
        what: 'API killed for running out of memory at 13.5 GB.',
        detail: 'Every request scanned the whole lake with its own DuckDB.',
      },
      {
        when: 'Oct 19 · 17:28 UTC',
        tone: 'down',
        what: 'Airflow scheduler exited.',
        detail: 'Under memory pressure Docker DNS could not resolve airflow-postgres; standalone mode never restarted it.',
      },
      {
        when: 'Oct 20 – Oct 31',
        tone: 'delayed',
        what: 'No nightly runs.',
        detail: 'Streaming kept writing raw data; the curated tables stayed on Oct 18.',
      },
      {
        when: 'Nov 1 · 04:09 UTC',
        tone: 'info',
        what: 'Pipeline Copilot flagged the stale data.',
        detail: 'Its diagnosis traced the gap to the missing scheduler.',
      },
      {
        when: 'Nov 1 · 04:25 UTC onward',
        tone: 'healthy',
        what: 'Fixed.',
        detail:
          'Airflow restarted, Oct 20–31 backfilled, and the API given a single-flight cache, partition pruning and a 2 GB memory cap.',
      },
    ],
  },
]
