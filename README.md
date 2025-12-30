# Speedrun WR Watch (Weekly)

This repo auto-updates the section below using the speedrun.com REST API.

It lists **current** #1 leaderboard runs (“world records” in practice) that were **verified in the last 7 days**.

<!-- WR-WEEKLY:START -->
_Updating..._
<!-- WR-WEEKLY:END -->

## How it works

- A GitHub Action runs daily.
- It builds and runs a small C program (`wr_weekly`) that:
  - fetches newly verified runs (`/runs?status=verified&orderby=verify-date&direction=desc`)
  - checks whether each run is currently #1 on its leaderboard (by calling the leaderboard endpoint with `top=1` and variable filters)
  - prints a Markdown table
- The workflow injects that Markdown into this README and commits if changed.

## Local run

On Ubuntu/Debian:

```bash
sudo apt-get update
sudo apt-get install -y build-essential libcurl4-openssl-dev libcjson-dev

make
./wr_weekly --days 7 --limit 50
