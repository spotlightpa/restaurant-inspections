# PA Restaurant Inspections

Scrapes Pennsylvania food safety inspection records, parses them, and publishes the data behind Spotlight PA's [Restaurant Safety Tracker](https://www.spotlightpa.org/restaurant-inspections/).

## Overview

The pipeline downloads statewide inspection data, cleans and merges it, adds enrichment such as violation risk levels, map coordinates and summaries of inspector comments, and publishes the result to S3, where the tracker reads it. It also notifies the site of new inspections and produces weekly county roundups.

Enrichment results are cached between runs, so each run only processes new records.

## Getting started

### Prerequisites

Use the Python version set in the GitHub Actions workflow, and have credentials for the services the pipeline connects to.

### Installation

```bash
git clone https://github.com/spotlightpa/restaurant-inspections
cd restaurant-inspections
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
playwright install chromium
```

### Configuration

Configuration comes from environment variables. The `env` block of the workflow in `.github/workflows/` is the authoritative list. Copy those names into a `.env` file in the project root and fill in the values.

### Usage

```bash
python scraper.py
```

By default the browser opens in a visible window. Set `CI=true` to run headless, as in GitHub Actions.

A local run uses production services, so it updates live data and sends real notifications.

## Project structure

- `scraper.py` is the entry point and runs the full pipeline.
- `helpers/` holds the individual pipeline steps.
- `counties/` holds scrapers for counties that publish inspections outside the state system. These run separately from the main pipeline.
- `.github/` holds the workflows and Dependabot configuration.

## Automation

The pipeline runs in GitHub Actions on the schedule and triggers defined in its workflow file, and can also be started by hand from the Actions tab.

The workflow makes a small keepalive commit on each scheduled run.

## Maintenance

The runtime environment and package versions are pinned, so they only change through a commit.

Dependabot opens pull requests for dependency updates. They are not merged automatically. Merge them when you can watch the next run, and check major version upgrades against the output of a known-good run.

When a run fails, check the run's logs in the Actions tab. Common causes are a change to the state's report that breaks the browser automation, an expired credential, or a run that exceeded GitHub's job time limit.
