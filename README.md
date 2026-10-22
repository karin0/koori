# koori

koori monitors a set of Linux servers and reports them in Telegram. It keeps one pinned message in a chat as a live panel, editing it every few seconds with each server's CPU, memory, disk, network, temperature and recent journal lines, and sends alerts to a chat when a server goes down or a resource crosses a threshold.

## Monitored servers

Each server needs:

- `journalctl` readable by the login user. koori streams `journalctl -f` from every server for the panel's log view.
- node_exporter listening on `127.0.0.1:9100`, with the collectors listed in `collectors` in [metrics.go](metrics.go) enabled. koori scrapes it through the SSH connection, so the port stays closed to the network.
- `ps`, which koori runs to attach the top processes to an alert.

A server is either `ssh`, reached over SSH with keys checked against `known_hosts`, or `local`, the machine koori runs on. See [config.example.yaml](config.example.yaml) for every option.

## Running

```sh
go install github.com/karin0/koori@latest
TELEGRAM_BOT_TOKEN=... koori [--debug] [--dry-run] [config.yaml]
```

| Variable | Meaning |
|---|---|
| `TELEGRAM_BOT_TOKEN` | Bot token. Required unless `--dry-run`. |
| `LOCAL_SERVER` | Base URL of a self-hosted telegram-bot-api server. Optional. |

`--dry-run` renders the panel to the terminal every 2 seconds and never contacts Telegram.

koori exits with status 1 when it cannot set up the panel message at startup, so a supervisor that restarts on failure retries it.

koori writes the ID of the panel message to `state.json` in the working directory, so a restart edits the same message. When editing fails with a bad request, for example after someone deleted the message, koori sends and pins a new one.

The bot needs permission to pin messages in the panel chat. The buttons under the panel halve or double the refresh interval (1 to 60 seconds), refresh immediately, and expand or collapse the journal lines.

## Alerts

koori evaluates alerts on every refresh and sends them to `alert_chat_id`. A server can raise:

- down, when the SSH session or journal stream fails;
- low available memory or root filesystem space;
- high and critical temperature;
- increments of kernel counters such as OOM kills and hung tasks.

Thresholds and the resend interval are constants at the top of [alert.go](alert.go). Every condition except a counter increment sends a recovery message once it clears.

## Development

[ci.sh](ci.sh) runs gofmt, `go vet`, the build and the tests.
