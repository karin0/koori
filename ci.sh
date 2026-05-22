#!/bin/bash
set -ex
paths=(koori)
uv sync --upgrade --exclude-newer '15 days'
uv run ruff check "${paths[@]}"
uv run ruff format --check "${paths[@]}"
exec uv run pyright "${paths[@]}"
