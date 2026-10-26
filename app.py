"""
Трансляция шахматной доски с chess.com.

Схема: на компьютере запущен обычный Chrome с --remote-debugging-port=9222,
в нём открыта партия на chess.com. Этот сервис подключается к вкладке по
Chrome DevTools Protocol (без Selenium и chromedriver), раз в N мс читает
расположение фигур прямо из DOM и отдаёт FEN на /get_fen. Страница / рисует
доску и обновляет её.
"""
import base64
import json
import os
import queue
import shutil
import socket
import struct
import subprocess
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from collections import deque

from flask import Flask, jsonify, render_template

app = Flask(__name__)

REMOTE_DEBUGGER = os.environ.get("REMOTE_DEBUGGER", "127.0.0.1:9222")
TAB_FILTER = os.environ.get("TAB_FILTER", "chess.com")
POLL_INTERVAL = float(os.environ.get("POLL_INTERVAL", "0.5"))

# Движок
_HERE = os.path.dirname(os.path.abspath(__file__))
_LOCAL_SF = os.path.join(_HERE, "engine", "stockfish")  # сюда кладёт get_stockfish.sh
ENGINE_PATH = (os.environ.get("ENGINE_PATH")
               or (_LOCAL_SF if os.access(_LOCAL_SF, os.X_OK) else None)
               or shutil.which("stockfish") or "/usr/games/stockfish")
ENGINE_DEPTH = int(os.environ.get("ENGINE_DEPTH", "30"))       # 0 = считать без ограничения
ENGINE_THREADS = int(os.environ.get("ENGINE_THREADS", str(max(1, (os.cpu_count() or 2) - 1))))
ENGINE_HASH = int(os.environ.get("ENGINE_HASH", "512"))        # МБ
ENGINE_LINES = int(os.environ.get("ENGINE_LINES", "3"))        # сколько лучших ходов показывать

# Задержка трансляции (секунды) для партий против живых соперников — как на
# турнирных трансляциях с оценкой движка: игрок не должен видеть подсказки
# во время своей же партии. Против компьютера и в анализе задержки нет.
LIVE_DELAY = max(60, int(os.environ.get("LIVE_DELAY", "180")))
NO_DELAY_PAGES = ("/play/computer", "/analysis")

START_FEN = "rnbqkbnr/pppppppp/8/8/8/8/PPPPPPPP/RNBQKBNR w KQkq - 0 1"

state = {
    "fen": START_FEN,
    "turn": "w",
    "flipped": False,
    "connected": False,
    "tab_url": None,
    "source": None,
    "status": "Запуск...",
    "updated": 0,
}
analysis = {"fen": None, "depth": 0, "cp": None, "mate": None, "pv": [], "lines": [],
            "engine": "запуск..."}
history = deque(maxlen=5000)  # (время, fen, turn, flipped) — только изменения позиции
state_lock = threading.Lock()


def set_state(**kw):
    with state_lock:
        state.update(kw)


def delay_for(url):
    if url and any(p in url for p in NO_DELAY_PAGES):
        return 0
    return LIVE_DELAY


def published():
    """Позиция, которую сейчас видят зрители (с учётом задержки)."""
    with state_lock:
        s = dict(state)
        hist = list(history)
    delay = delay_for(s["tab_url"])
    cutoff = time.time() - delay
    shown = None
    for entry in reversed(hist):
        if entry[0] <= cutoff:
            shown = entry
            break
    s["delay"] = delay
    if shown:
        s["fen"], s["turn"], s["flipped"] = shown[1], shown[2], shown[3]
        s["waiting"] = 0
    else:
        s["fen"], s["turn"] = START_FEN, "w"
        s["waiting"] = int(hist[0][0] - cutoff) + 1 if hist else delay
    return s


# ---------------------------------------------------------------------------
# Минимальный WebSocket-клиент (RFC 6455) — чтобы не тянуть зависимости.
# ---------------------------------------------------------------------------
class MiniWebSocket:
    def __init__(self, url, timeout=10):
        u = urllib.parse.urlparse(url)
        port = u.port or 80
        self.sock = socket.create_connection((u.hostname, port), timeout=timeout)
        key = base64.b64encode(os.urandom(16)).decode()
        path = u.path + ("?" + u.query if u.query else "")
        # Origin не отправляем — тогда Chrome не требует --remote-allow-origins.
        req = (
            f"GET {path} HTTP/1.1\r\n"
            f"Host: {u.hostname}:{port}\r\n"
            "Upgrade: websocket\r\n"
            "Connection: Upgrade\r\n"
            f"Sec-WebSocket-Key: {key}\r\n"
            "Sec-WebSocket-Version: 13\r\n\r\n"
        )
        self.sock.sendall(req.encode())
        self.buf = b""
        while b"\r\n\r\n" not in self.buf:
            chunk = self.sock.recv(4096)
            if not chunk:
                raise ConnectionError("WebSocket: соединение закрыто при рукопожатии")
            self.buf += chunk
        head, self.buf = self.buf.split(b"\r\n\r\n", 1)
        status_line = head.split(b"\r\n", 1)[0].decode(errors="replace")
        if " 101 " not in status_line + " ":
            raise ConnectionError(f"WebSocket: неожиданный ответ: {status_line}")

    def _read(self, n):
        while len(self.buf) < n:
            chunk = self.sock.recv(65536)
            if not chunk:
                raise ConnectionError("WebSocket: соединение закрыто")
            self.buf += chunk
        data, self.buf = self.buf[:n], self.buf[n:]
        return data

    def _send_frame(self, opcode, payload):
        header = bytes([0x80 | opcode])
        n = len(payload)
        if n < 126:
            header += bytes([0x80 | n])
        elif n < 65536:
            header += bytes([0x80 | 126]) + struct.pack(">H", n)
        else:
            header += bytes([0x80 | 127]) + struct.pack(">Q", n)
        mask = os.urandom(4)
        masked = bytes(b ^ mask[i % 4] for i, b in enumerate(payload))
        self.sock.sendall(header + mask + masked)

    def send(self, text):
        self._send_frame(0x1, text.encode())

    def recv(self):
        message = b""
        while True:
            b1, b2 = self._read(2)
            fin, opcode = b1 & 0x80, b1 & 0x0F
            n = b2 & 0x7F
            if n == 126:
                n = struct.unpack(">H", self._read(2))[0]
            elif n == 127:
                n = struct.unpack(">Q", self._read(8))[0]
            mask = self._read(4) if b2 & 0x80 else None
            payload = self._read(n)
            if mask:
                payload = bytes(b ^ mask[i % 4] for i, b in enumerate(payload))
            if opcode == 0x8:
                raise ConnectionError("WebSocket: сервер закрыл соединение")
            if opcode == 0x9:  # ping
                self._send_frame(0xA, payload)
                continue
            if opcode == 0xA:  # pong
                continue
            message += payload
            if fin:
                return message.decode()

    def close(self):
        try:
            self.sock.close()
        except Exception:
            pass


# ---------------------------------------------------------------------------
# Работа с Chrome DevTools Protocol
# ---------------------------------------------------------------------------
_no_proxy = urllib.request.build_opener(urllib.request.ProxyHandler({}))


def debugger_base():
    """Chrome отвечает только на Host = IP или localhost, поэтому имя
    (например host.docker.internal) превращаем в IP."""
    host, _, port = REMOTE_DEBUGGER.rpartition(":")
    try:
        ip = socket.gethostbyname(host)
    except socket.gaierror as e:
        raise ConnectionError(f"не удалось разрешить {host}: {e}")
    return f"{ip}:{port}"


def find_tab():
    base = debugger_base()
    with _no_proxy.open(f"http://{base}/json/list", timeout=5) as r:
        tabs = json.load(r)
    pages = [t for t in tabs if t.get("type") == "page" and TAB_FILTER in t.get("url", "")]
    if not pages:
        return None
    # Предпочитаем вкладки с самой партией.
    pages.sort(key=lambda t: not any(k in t["url"] for k in ("/game", "/play", "/live", "/analysis")))
    tab = pages[0]
    ws_url = tab["webSocketDebuggerUrl"]
    # На всякий случай подставляем тот же IP, по которому ходили за списком.
    ws_url = f"ws://{base}/devtools/" + ws_url.split("/devtools/", 1)[1]
    return tab["url"], ws_url


class CDP:
    def __init__(self, ws_url):
        self.ws = MiniWebSocket(ws_url)
        self.msg_id = 0

    def call(self, method, **params):
        self.msg_id += 1
        my_id = self.msg_id
        self.ws.send(json.dumps({"id": my_id, "method": method, "params": params}))
        while True:
            msg = json.loads(self.ws.recv())
            if msg.get("id") == my_id:
                if "error" in msg:
                    raise RuntimeError(msg["error"])
                return msg.get("result", {})

    def evaluate(self, expression):
        res = self.call("Runtime.evaluate", expression=expression, returnByValue=True)
        if "exceptionDetails" in res:
            raise RuntimeError(res["exceptionDetails"].get("text", "JS error"))
        return res.get("result", {}).get("value")

    def close(self):
        self.ws.close()


# JS, который выполняется на странице chess.com и возвращает всё одним вызовом
# (быстрее, чем дёргать каждый элемент по отдельности).
READ_BOARD_JS = r"""
(() => {
  const board = document.querySelector('wc-chess-board, chess-board, #board-single, #board-play-computer, .board');
  if (!board) return { ok: false, reason: 'доска не найдена на странице' };
  const root = board.shadowRoot || board;
  const sq = (cls) => { const m = cls.match(/\bsquare-(\d)(\d)\b/); return m ? [+m[1], +m[2]] : null; };
  const pieces = [];
  for (const el of root.querySelectorAll('.piece')) {
    const cls = el.getAttribute('class') || '';
    const t = cls.match(/\b([wb][pnbrqk])\b/);
    const s = sq(cls);
    if (t && s && !cls.includes('dragging')) pieces.push([t[1], s[0], s[1]]);
  }
  const highlights = [];
  for (const el of root.querySelectorAll('.highlight')) {
    const s = sq(el.getAttribute('class') || '');
    if (s) highlights.push(s);
  }
  // Точная позиция из объекта партии chess.com: очередь хода, рокировки,
  // взятие на проходе. Разметка доски — только запасной вариант.
  let gameFen = null;
  try {
    const g = board.game;
    if (g && typeof g.getFEN === 'function') gameFen = g.getFEN();
  } catch (e) {}
  return { ok: true, pieces, highlights, gameFen, flipped: board.classList.contains('flipped') };
})()
"""


def valid_fen(fen):
    if not isinstance(fen, str):
        return False
    parts = fen.split()
    if len(parts) < 4 or parts[1] not in ("w", "b"):
        return False
    rows = parts[0].split("/")
    if len(rows) != 8:
        return False
    for row in rows:
        n = 0
        for c in row:
            if c.isdigit():
                n += int(c)
            elif c in "pnbrqkPNBRQK":
                n += 1
            else:
                return False
        if n != 8:
            return False
    return True


def build_fen(data):
    grid = [["" for _ in range(8)] for _ in range(8)]  # grid[rank][file]
    for ptype, f, r in data["pieces"]:
        if 1 <= f <= 8 and 1 <= r <= 8:
            grid[r - 1][f - 1] = ptype[1].upper() if ptype[0] == "w" else ptype[1]

    rows = []
    for rank in range(7, -1, -1):
        row, empty = "", 0
        for file in range(8):
            p = grid[rank][file]
            if not p:
                empty += 1
            else:
                if empty:
                    row += str(empty)
                    empty = 0
                row += p
        rows.append(row + (str(empty) if empty else ""))

    # Очередь хода: подсвечены 2 клетки последнего хода; фигура стоит на одной
    # из них — её цвет только что ходил, значит ходит соперник.
    turn = "w"
    for f, r in data.get("highlights", []):
        p = grid[r - 1][f - 1] if 1 <= f <= 8 and 1 <= r <= 8 else ""
        if p:
            turn = "b" if p.isupper() else "w"
            break

    # Права на рокировку — по положению королей и ладей (приблизительно).
    castling = ""
    if grid[0][4] == "K":
        castling += "K" if grid[0][7] == "R" else ""
        castling += "Q" if grid[0][0] == "R" else ""
    if grid[7][4] == "k":
        castling += "k" if grid[7][7] == "r" else ""
        castling += "q" if grid[7][0] == "r" else ""

    return "/".join(rows) + f" {turn} {castling or '-'} - 0 1", turn


def scrape_loop():
    while True:
        cdp = None
        try:
            set_state(connected=False, status=f"Подключаюсь к Chrome ({REMOTE_DEBUGGER})...")
            tab = find_tab()
            if not tab:
                set_state(status=f"В Chrome нет вкладки с «{TAB_FILTER}». Откройте партию.")
                time.sleep(2)
                continue
            url, ws_url = tab
            cdp = CDP(ws_url)
            print(f"Подключено к вкладке: {url}", flush=True)
            set_state(connected=True, tab_url=url, status="Подключено")

            last_fen = None
            while True:
                data = cdp.evaluate(READ_BOARD_JS)
                if not data or not data.get("ok"):
                    set_state(status=(data or {}).get("reason", "нет данных"))
                elif data["pieces"] or valid_fen(data.get("gameFen")):
                    if valid_fen(data.get("gameFen")):
                        fen = " ".join(data["gameFen"].split()[:6])
                        turn, source = fen.split()[1], "chess.com"
                    else:
                        (fen, turn), source = build_fen(data), "разметка доски (приблизительно)"
                    now = time.time()
                    set_state(fen=fen, turn=turn, flipped=data["flipped"], source=source,
                              status="Трансляция идёт", updated=now)
                    if fen != last_fen:
                        print("FEN:", fen, flush=True)
                        last_fen = fen
                        with state_lock:
                            history.append((now, fen, turn, data["flipped"]))
                time.sleep(POLL_INTERVAL)
                # Если вкладку закрыли или ушли с chess.com — найти заново.
                cur = cdp.evaluate("location.href")
                if TAB_FILTER not in (cur or ""):
                    raise ConnectionError("вкладка ушла со страницы партии")
                set_state(tab_url=cur)

        except Exception as e:
            msg = str(e) or e.__class__.__name__
            if isinstance(e, (ConnectionRefusedError, urllib.error.URLError)):
                msg = (f"Chrome не отвечает на {REMOTE_DEBUGGER}. Запущен ли он с "
                       f"--remote-debugging-port=9222 --user-data-dir=...?")
            print("Ошибка:", msg, flush=True)
            set_state(connected=False, status=msg)
            time.sleep(2)
        finally:
            if cdp:
                cdp.close()


# ---------------------------------------------------------------------------
# Движок Stockfish (UCI) — анализирует позицию, которую видят зрители
# ---------------------------------------------------------------------------
def set_analysis(**kw):
    with state_lock:
        analysis.update(kw)


def plausible(fen):
    board = fen.split()[0]
    return board.count("K") == 1 and board.count("k") == 1


class UCIEngine:
    def __init__(self, path):
        self.proc = subprocess.Popen([path], stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                     stderr=subprocess.DEVNULL, text=True, bufsize=1)
        self.lines = queue.Queue()
        threading.Thread(target=self._reader, daemon=True).start()
        self.send("uci")
        self.name = "Stockfish"
        for line in self._until("uciok", 10):
            if line.startswith("id name "):
                self.name = line[8:]
        self.send(f"setoption name Threads value {ENGINE_THREADS}")
        self.send(f"setoption name Hash value {ENGINE_HASH}")
        self.send(f"setoption name MultiPV value {ENGINE_LINES}")
        self.send("isready")
        list(self._until("readyok", 10))

    def _reader(self):
        for line in self.proc.stdout:
            self.lines.put(line.strip())
        self.lines.put(None)  # процесс завершился

    def _until(self, token, timeout):
        end = time.time() + timeout
        while True:
            line = self.lines.get(timeout=max(0.01, end - time.time()))
            if line is None:
                raise RuntimeError("движок завершился")
            yield line
            if line.startswith(token):
                return

    def send(self, cmd):
        self.proc.stdin.write(cmd + "\n")
        self.proc.stdin.flush()

    def close(self):
        try:
            self.proc.kill()
        except Exception:
            pass


def parse_info(line):
    """'info depth 14 multipv 2 score cp 31 ... pv e2e4 e7e5' -> (n, depth, cp, mate, pv)"""
    t = line.split()
    if "pv" not in t or "score" not in t or "lowerbound" in t or "upperbound" in t:
        return None
    n = int(t[t.index("multipv") + 1]) if "multipv" in t else 1
    depth = int(t[t.index("depth") + 1]) if "depth" in t else 0
    i = t.index("score")
    kind, val = t[i + 1], int(t[i + 2])
    pv = t[t.index("pv") + 1:]
    return n, depth, (val if kind == "cp" else None), (val if kind == "mate" else None), pv


def engine_loop():
    while True:
        engine = None
        try:
            engine = UCIEngine(ENGINE_PATH)
            print("Движок:", engine.name, flush=True)
            set_analysis(engine=engine.name)
            current = None      # позиция, которую считает движок
            stopping = False    # ждём bestmove от прерванного поиска
            searching = False
            lines = {}          # номер варианта -> {depth, cp, mate, pv}
            go = f"go depth {ENGINE_DEPTH}" if ENGINE_DEPTH > 0 else "go infinite"
            while True:
                target = published()["fen"]
                if target != current and not stopping:
                    if searching:
                        engine.send("stop")
                        stopping = True
                    elif plausible(target):
                        engine.send(f"position fen {target}")
                        engine.send(go)
                        current, searching, lines = target, True, {}
                        set_analysis(fen=target, depth=0, cp=None, mate=None, pv=[], lines=[])
                    else:
                        current = target
                try:
                    line = engine.lines.get(timeout=0.2)
                except queue.Empty:
                    continue
                if line is None:
                    raise RuntimeError("движок завершился")
                if line.startswith("bestmove"):
                    searching = stopping = False
                    if current and (line.split() + [""])[1] in ("(none)", "0000"):
                        set_analysis(pv=[], lines=[], depth=max(1, analysis["depth"]))
                elif line.startswith("info") and not stopping:
                    info = parse_info(line)
                    if info:
                        n, depth, cp, mate, pv = info
                        # Движок даёт оценку за сторону, которая ходит; переводим к белым.
                        sign = 1 if current.split()[1] == "w" else -1
                        lines[n] = {"depth": depth, "pv": pv,
                                    "cp": None if cp is None else sign * cp,
                                    "mate": None if mate is None else sign * mate}
                        # Варианты, не обновлённые на этой глубине, отбрасываем,
                        # чтобы не смешивать старые и новые оценки.
                        top = lines.get(1)
                        if top:
                            ordered = [lines[k] for k in sorted(lines)
                                       if lines[k]["depth"] >= top["depth"] - 1]
                            set_analysis(depth=top["depth"], pv=top["pv"], cp=top["cp"],
                                         mate=top["mate"], lines=ordered)
        except FileNotFoundError:
            set_analysis(engine=f"Stockfish не найден ({ENGINE_PATH}). Запустите ./get_stockfish.sh")
            print("Stockfish не найден:", ENGINE_PATH, flush=True)
            time.sleep(10)
        except Exception as e:
            print("Ошибка движка:", e, flush=True)
            set_analysis(engine=f"ошибка движка: {e}")
            time.sleep(2)
        finally:
            if engine:
                engine.close()


@app.route("/")
def index():
    return render_template("index.html")


@app.route("/get_fen")
def get_fen():
    s = published()
    with state_lock:
        a = dict(analysis)
    s["analysis"] = a if a["fen"] == s["fen"] else {"engine": a["engine"], "fen": None}
    return jsonify(s)


if __name__ == "__main__":
    threading.Thread(target=scrape_loop, daemon=True).start()
    threading.Thread(target=engine_loop, daemon=True).start()
    port = int(os.environ.get("FLASK_PORT", 5000))
    print(f"Flask запущен на 0.0.0.0:{port}", flush=True)
    app.run(host="0.0.0.0", port=port, debug=False, threaded=True)