# Setup guide

This guide starts from a machine with nothing installed and ends with the test
suite passing and the server running. Every command here works with this
repository as it stands.

**The only hard requirement is Python 3.11 or newer.** The server has no runtime
dependencies. Everything else on this page is either a check or a development
tool.

---

## Step 1 — Identify your operating system

You need this only to pick the right column in the steps below.

### Windows (PowerShell)

```powershell
systeminfo | Select-String "OS Name","OS Version","System Type"
```

### Linux

```bash
uname -a
```

```bash
cat /etc/os-release
```

### macOS

```bash
sw_vers
```

```bash
uname -m
```

`uname -m`/`System Type` tells you whether you are on `x86_64`/`AMD64` or
`arm64`/`aarch64`, which matters only when downloading a Python installer.

---

## Step 2 — Check Python

**Required version: 3.11 or newer.**

The project uses `X | Y` union syntax in annotations evaluated at runtime by
dataclasses, `asyncio.TimeoutError` unified with the builtin `TimeoutError`
(3.11), `dataclass(slots=True)`, and `zip(..., strict=True)`. Python 3.10 and
earlier will not run it.

### Windows

```powershell
python --version
```

If that fails or reports something older, try the launcher:

```powershell
py -3 --version
```

```powershell
py --list
```

### Linux / macOS

```bash
python3 --version
```

```bash
which python3
```

### Reading the result

| Output | What to do |
| ------ | ---------- |
| `Python 3.11.x` or newer | You are ready. Go to Step 4. |
| `Python 3.10.x` or older | Install a newer Python — Step 3. |
| `command not found` / not recognised | Install Python — Step 3. |
| Windows opens the Microsoft Store | The `python` alias is a stub. Install properly — Step 3. |

---

## Step 3 — Install Python (only if Step 2 said you need to)

### Windows

Download the installer from <https://www.python.org/downloads/windows/> and
choose a 3.11+ release.

**During installation, tick "Add python.exe to PATH".** That one checkbox is the
difference between the commands on this page working and not working.

Or, if you have winget:

```powershell
winget install --id Python.Python.3.12 -e
```

Close and reopen the terminal afterwards so the new `PATH` takes effect, then
re-run the Step 2 check.

### macOS

With Homebrew:

```bash
brew install python@3.12
```

Without Homebrew, download the macOS installer from
<https://www.python.org/downloads/macos/>.

### Debian / Ubuntu

```bash
sudo apt update
```

```bash
sudo apt install -y python3 python3-venv python3-pip
```

`python3-venv` is a separate package on Debian and Ubuntu, and without it
`python3 -m venv` fails with a confusing message. Install it now.

If your distribution ships a Python older than 3.11:

```bash
sudo add-apt-repository ppa:deadsnakes/ppa
```

```bash
sudo apt update && sudo apt install -y python3.12 python3.12-venv
```

### Fedora / RHEL

```bash
sudo dnf install -y python3 python3-pip
```

### Arch

```bash
sudo pacman -S python python-pip
```

### Verify

```bash
python3 --version
```

```powershell
python --version
```

You should see 3.11 or newer.

---

## Step 4 — Check the other tools

### Required

Only Python is required. `pip` and `venv` ship with it; confirm:

```bash
python3 -m pip --version
```

```bash
python3 -m venv --help
```

If `venv` is missing on Debian/Ubuntu, install `python3-venv` (Step 3).

### Optional

These are useful but nothing here depends on them.

| Tool | Why you might want it | Check | Install |
| ---- | --------------------- | ----- | ------- |
| `git` | Cloning and publishing the repository | `git --version` | [git-scm.com/downloads](https://git-scm.com/downloads), `sudo apt install git`, `brew install git` |
| `redis-cli` | A familiar interactive client | `redis-cli --version` | `sudo apt install redis-tools`, `brew install redis` |
| `redis-server` | Comparing behaviour and benchmarks against real Redis | `redis-server --version` | `sudo apt install redis-server`, `brew install redis` |
| `nc` / `telnet` | Poking the server by hand (inline commands are supported) | `nc -h` | usually preinstalled |

`redis-cli` and `redis-server` are **development conveniences only**. The server
does not need them, the tests do not need them, and nothing in this repository
imports or shells out to them.

### Development dependencies

Installed by `pip install -e ".[dev]"` in Step 5:

| Package | Purpose |
| ------- | ------- |
| `pytest` | Test runner |
| `ruff` | Linter and formatter |
| `mypy` | Static type checker |

---

## Step 5 — Create the environment and install

From the project directory (the one containing `pyproject.toml`).

### Create a virtual environment

A virtual environment keeps this project's packages out of your system Python.

Windows:

```powershell
python -m venv .venv
```

Linux / macOS:

```bash
python3 -m venv .venv
```

### Activate it

Windows PowerShell:

```powershell
.venv\Scripts\Activate.ps1
```

Windows Command Prompt:

```text
.venv\Scripts\activate.bat
```

Linux / macOS:

```bash
source .venv/bin/activate
```

Your prompt should now start with `(.venv)`.

> **PowerShell refuses to run the activation script?** Windows blocks unsigned
> scripts by default. Allow them for your user only:
>
> ```powershell
> Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope CurrentUser
> ```

### Upgrade the packaging tools

```bash
python -m pip install --upgrade pip setuptools wheel
```

### Install the project

To run the server only:

```bash
python -m pip install -e .
```

To run the tests and the tooling as well — which is what you want for
development:

```bash
python -m pip install -e ".[dev]"
```

`-e` installs in editable mode, so your edits to `src/` take effect immediately
without reinstalling.

---

## Step 6 — Verify the installation

Run these in order. Each one should succeed before you move to the next.

### 1. The right Python is active

```bash
python --version
```

Expect 3.11 or newer.

### 2. The virtual environment is active

Windows:

```powershell
(Get-Command python).Source
```

Linux / macOS:

```bash
which python
```

The path should be inside `.venv`.

### 3. Dependencies are installed

```bash
python -m pip list
```

You should see `pyredis-server`, `pytest`, `ruff` and `mypy`.

### 4. The package imports

```bash
python -c "import pyredis; print(pyredis.__version__)"
```

Expect `1.0.0`.

### 5. The command-line entry point works

```bash
pyredis-server --version
```

```bash
pyredis-server --help
```

### 6. The tests run

Fast unit tests first:

```bash
python -m pytest tests/unit -q
```

Then everything, including the tests that start real server processes:

```bash
python -m pytest -q
```

Expect `585 passed`. The full run takes roughly two minutes, most of it in the
replication tests, which start pairs of real servers and wait on real timeouts.

### 7. The server starts

```bash
pyredis-server --port 6379
```

You should see a line like:

```text
12:00:00 INFO     pyredis.server: pyredis 1.0.0 ready to accept connections on 127.0.0.1:6379 (role=master, pid=1234)
```

Stop it with `Ctrl+C`.

---

## Running the server

```bash
pyredis-server
```

```bash
pyredis-server --port 6380
```

```bash
pyredis-server --port 6379 --dir ./data --dbfilename dump.rdb
```

```bash
pyredis-server --port 6380 --replicaof "localhost 6379"
```

```bash
python -m pyredis --port 0 --loglevel debug
```

`python -m pyredis` and `pyredis-server` are the same program; the module form
works without the package being installed, as long as `src/` is importable.

Stop the server with `Ctrl+C`. It shuts down gracefully: background tasks are
cancelled, open connections are closed, and the listening socket is released.

---

## Connecting to it

### With redis-cli

```bash
redis-cli -p 6379
```

```bash
redis-cli -p 6379 SET name aryan
```

```bash
redis-cli -p 6379 GET name
```

### With redis-py

```bash
python -m pip install redis
```

```python
import redis

client = redis.Redis(host="127.0.0.1", port=6379)
client.set("name", "aryan")
print(client.get("name"))  # b'aryan'
print(client.ttl("name"))  # -1
```

### With nc or telnet

Inline commands are supported, so a plain terminal works:

```bash
printf 'PING\r\nSET name aryan\r\nGET name\r\n' | nc 127.0.0.1 6379
```

### With nothing but Python

```python
import socket

sock = socket.create_connection(("127.0.0.1", 6379))
sock.sendall(b"*1\r\n$4\r\nPING\r\n")
print(sock.recv(1024))  # b'+PONG\r\n'
sock.close()
```

---

## Running the tests

```bash
pytest                          # everything (585 tests, about two minutes)
pytest tests/unit               # unit tests only (fast, no sockets)
pytest tests/integration        # integration tests only
pytest -m "not slow"            # skip the timing-sensitive tests
pytest -m integration           # only the tests that use real sockets
pytest -v                       # show every test name
pytest -x                       # stop at the first failure
pytest --lf                     # re-run only what failed last time
pytest tests/unit/test_store.py::TestExpiry -v      # one class
```

### Linting, formatting and type checking

```bash
ruff format .                   # reformat in place
ruff format --check .           # fail if anything is unformatted
ruff check .                    # lint
ruff check --fix .              # lint and fix what can be fixed
mypy                            # strict type check of src and tests
```

All four at once:

```bash
bash scripts/check.sh
```

```powershell
powershell -File scripts/check.ps1
```

### Benchmarks

Start a server, then in another terminal:

```bash
python benchmarks/bench.py --port 6379
```

```bash
python benchmarks/bench.py --port 6379 --clients 50 --requests 20000 --pipeline 16
```

The script works against any RESP server, so pointing it at a real
`redis-server` on the same machine gives a fair comparison.

---

## Troubleshooting

### `Address already in use` / `OSError: [WinError 10048]`

Something is already listening on that port — often a previous run.

Find it on Linux/macOS:

```bash
lsof -i :6379
```

On Windows:

```powershell
Get-NetTCPConnection -LocalPort 6379 | Select-Object OwningProcess
```

Then either stop that process or use a different port:

```bash
pyredis-server --port 6380
```

### `ModuleNotFoundError: No module named 'pyredis'`

The virtual environment is not active, or the package is not installed. Activate
it and reinstall:

```bash
python -m pip install -e ".[dev]"
```

### `pyredis-server: command not found`

The entry-point script lives in the virtual environment's `bin`/`Scripts`
directory, which is only on `PATH` while the environment is active. Either
activate it, or use the module form, which always works:

```bash
python -m pyredis --port 6379
```

### PowerShell: "running scripts is disabled on this system"

See the note in Step 5. `Set-ExecutionPolicy RemoteSigned -Scope CurrentUser`
fixes it for your user only.

### Tests fail with timeouts

The integration tests start real subprocesses and wait on real timers. On a very
loaded machine the slow tests can time out. Run the deterministic subset to
confirm the logic is sound:

```bash
pytest -m "not slow"
```

Then re-run the full suite on an idle machine.

### The server exits immediately with "corrupt snapshot"

There is an unreadable `dump.rdb` in `--dir`. That is deliberate: starting with
an empty keyspace would hide the problem, and the next `SAVE` would overwrite the
file. Move the bad file aside and start again:

```bash
mv dump.rdb dump.rdb.broken
```

### A replica logs "replication link failed" repeatedly

The master is not reachable. Check that it is running and that the host and port
in `--replicaof` are right. The replica retries with exponential backoff up to
16 seconds, so it will recover on its own once the master is back.

### Connecting from another machine fails

The server binds to `127.0.0.1` by default and is therefore unreachable from the
network. That is intentional — there is no authentication. If you understand the
risk and are on a trusted network:

```bash
pyredis-server --host 0.0.0.0 --port 6379
```

### Debugging

Turn up the logging:

```bash
python -m pyredis --port 6379 --loglevel debug
```

Debug level logs every connection, disconnection, active-expiry sweep and
replication event.

To watch the wire protocol itself, connect with `nc` and type commands by hand —
inline commands are supported, and the raw replies are printed as they arrive.
