# koori

koori monitors a set of Linux servers and reports them in Telegram. It keeps a pinned [widget](https://core.telegram.org/bots/widgets) in the private chat with the bot as a live panel of each server's CPU, memory, disk, network and temperature, with the CPU usage of the last 10 minutes, keeps the recent journal lines in a message above it, and sends alerts to a chat when a server goes down or a resource crosses a threshold.

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
| `LOCAL_SERVER` | Base URL of a self-hosted telegram-bot-api server, which must support Bot API 10.4. Optional. |

`--dry-run` prints the panel data and the newest journal lines to the terminal and never contacts Telegram.

`chat_id` must be the private chat with the bot, the only chat where Telegram accepts widgets. koori scrapes every server every 5 seconds. The widget asks koori for the latest metrics when it appears on screen and then every 5 seconds, the fastest pace Telegram allows, so koori sends nothing to Telegram for the panel while nobody looks at it. Telegram limits the answer to 1024 bytes and the widget to 600 px in height, which fit about three servers. koori drops the servers that do not fit the answer and logs an error, and the widget cuts off the servers below its height. koori edits the journal message only when its lines change, and the button under it expands or collapses them.

At startup koori sends the journal message and a new widget, since a sent widget keeps its HTML, pins the widget, and deletes the messages of the previous run, whose IDs it keeps in `state.json` in the working directory. koori exits with status 1 when it cannot set up these messages, so a supervisor that restarts on failure retries it.

## Alerts

koori evaluates alerts after every scrape and sends them to `alert_chat_id`. A server can raise:

- down, when the SSH session or journal stream fails;
- low available memory or root filesystem space;
- high and critical temperature;
- increments of kernel counters such as OOM kills and hung tasks.

Thresholds and the resend interval are constants at the top of [alert.go](alert.go). Every condition except a counter increment sends a recovery message once it clears.

## Development

[ci.sh](ci.sh) runs gofmt, `go vet`, the build and the tests.
