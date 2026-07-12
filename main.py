# -*- coding: utf-8 -*-
"""
Синхронизированный секундомер (Kivy + UDP broadcast), с автопоиском устройств
в локальной сети и разбивкой времени на годы/месяцы/дни/часы/минуты/секунды.

Работает одинаково и на Android, и на Windows/Linux/macOS (десктоп) — код
один и тот же, просто собирается по-разному (buildozer -> APK, pyinstaller -> EXE).

Сетевой протокол (UDP, порт 5005, broadcast):
- "hello"  — рассылается сразу при запуске приложения: "я тут, кто ещё есть?"
- "state"  — рассылается при старте/стопе/сбросе, в ответ на "hello" от
             кого-то другого, и раз в 3 сек "на всякий случай" (heartbeat).
             Содержит running/start_time/accumulated/origin_time/changed_at.

Разрешение конфликтов между устройствами: last-writer-wins по полю
changed_at (у кого событие произошло позже по времени — то и применяется
у всех). Список "устройств рядом" — это IP-адреса, от которых приходили
любые сообщения за последние 8 секунд.
"""

import calendar
import json
import os
import socket
import threading
import time
from datetime import datetime

from kivy.app import App
from kivy.clock import Clock
from kivy.uix.boxlayout import BoxLayout
from kivy.uix.label import Label
from kivy.uix.button import Button
from kivy.uix.textinput import TextInput
from kivy.metrics import sp

UDP_PORT = 5005
BROADCAST_ADDR = "255.255.255.255"
HEARTBEAT_INTERVAL = 3.0
PEER_TIMEOUT = 8.0

WEEKDAYS_RU = ["понедельник", "вторник", "среда", "четверг", "пятница", "суббота", "воскресенье"]
MONTHS_RU_GEN = [
    "января", "февраля", "марта", "апреля", "мая", "июня",
    "июля", "августа", "сентября", "октября", "ноября", "декабря",
]


def default_state():
    return {
        "name": "Мой секундомер",  # название — для чего он, синхронизируется вместе со временем
        "running": False,
        "start_time": None,      # когда стартовала текущая сессия (unix time)
        "accumulated": 0.0,      # сколько секунд уже накоплено до текущей сессии
        "origin_time": None,     # когда счёт был запущен впервые (для отображения даты старта)
        "changed_at": 0.0,       # момент последнего изменения — для разрешения конфликтов
    }


def get_elapsed_seconds(state):
    if state["running"] and state["start_time"] is not None:
        return state["accumulated"] + (time.time() - state["start_time"])
    return state["accumulated"]


def format_hms_short(total_seconds):
    total_seconds = max(0, int(total_seconds))
    days, rem = divmod(total_seconds, 86400)
    hours, rem = divmod(rem, 3600)
    minutes, seconds = divmod(rem, 60)
    if days > 0:
        return f"{days} дн. {hours:02d}:{minutes:02d}:{seconds:02d}"
    return f"{hours:02d}:{minutes:02d}:{seconds:02d}"


def format_duration_breakdown(total_seconds):
    """Приблизительная разбивка длительности на годы/месяцы/дни/часы/мин/сек.
    Approximation: год = 365 дней, месяц = 30 дней (для удобочитаемости,
    без привязки к конкретному календарю, т.к. секундомер может ставиться
    на паузу и не идёт "по календарю")."""
    total_seconds = max(0, int(total_seconds))
    days_total, rem = divmod(total_seconds, 86400)
    years, days_total = divmod(days_total, 365)
    months, days = divmod(days_total, 30)
    hours, rem = divmod(rem, 3600)
    minutes, seconds = divmod(rem, 60)

    parts = []
    if years:
        parts.append(f"{years} г.")
    if months or years:
        parts.append(f"{months} мес.")
    parts.append(f"{days} дн.")
    parts.append(f"{hours:02d}:{minutes:02d}:{seconds:02d}")
    return " ".join(parts)


def format_origin_datetime(unix_ts):
    dt = datetime.fromtimestamp(unix_ts)
    weekday = WEEKDAYS_RU[dt.weekday()]
    month = MONTHS_RU_GEN[dt.month - 1]
    return f"{dt.day} {month} {dt.year}, {dt.strftime('%H:%M:%S')} ({weekday})"


class NetworkSync:
    """UDP-приём/рассылка состояния и автообнаружение устройств в сети."""

    def __init__(self, on_state_received, on_hello_received, on_peer_seen):
        self.on_state_received = on_state_received
        self.on_hello_received = on_hello_received
        self.on_peer_seen = on_peer_seen
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        try:
            self.sock.bind(("", UDP_PORT))
        except OSError:
            self.sock.bind(("", 0))
        self.running = True
        self.listener_thread = threading.Thread(target=self._listen, daemon=True)
        self.listener_thread.start()

    def _listen(self):
        self.sock.settimeout(1.0)
        while self.running:
            try:
                data, addr = self.sock.recvfrom(4096)
            except socket.timeout:
                continue
            except OSError:
                break
            try:
                msg = json.loads(data.decode("utf-8"))
            except json.JSONDecodeError:
                continue

            self.on_peer_seen(addr[0])

            msg_type = msg.get("type")
            if msg_type == "state":
                self.on_state_received(msg.get("state", {}))
            elif msg_type == "hello":
                self.on_hello_received()

    def send(self, msg_type, state=None):
        payload = {"type": msg_type}
        if state is not None:
            payload["state"] = state
        data = json.dumps(payload).encode("utf-8")
        try:
            self.sock.sendto(data, (BROADCAST_ADDR, UDP_PORT))
        except OSError:
            pass  # сети нет — просто работаем локально

    def close(self):
        self.running = False
        self.sock.close()


class StopwatchApp(App):
    def build(self):
        self.state_file = os.path.join(self.user_data_dir, "stopwatch_state.json")
        os.makedirs(self.user_data_dir, exist_ok=True)
        self.state = self.load_state()
        self.peers = {}  # ip -> время последнего сообщения от него

        self.net = NetworkSync(
            on_state_received=self.on_network_state,
            on_hello_received=self.on_hello,
            on_peer_seen=self.on_peer_seen,
        )

        root = BoxLayout(orientation="vertical", padding=sp(24), spacing=sp(10))

        self.name_input = TextInput(
            text=self.state.get("name", "Мой секундомер"),
            font_size=sp(20),
            multiline=False,
            size_hint=(1, 0.14),
            halign="center",
            padding=[sp(10), sp(14), sp(10), sp(14)],
        )
        self.name_input.bind(on_text_validate=lambda *_: self.on_name_changed())
        self.name_input.bind(focus=lambda instance, has_focus: (not has_focus) and self.on_name_changed())
        root.add_widget(self.name_input)

        self.time_label = Label(text="00:00:00", font_size=sp(42), bold=True, size_hint=(1, 0.20))
        root.add_widget(self.time_label)

        self.breakdown_label = Label(text="", font_size=sp(16), size_hint=(1, 0.12))
        root.add_widget(self.breakdown_label)

        self.origin_label = Label(text="", font_size=sp(13), size_hint=(1, 0.12), color=(0.7, 0.7, 0.7, 1))
        root.add_widget(self.origin_label)

        self.status_label = Label(text="", font_size=sp(14), size_hint=(1, 0.1))
        root.add_widget(self.status_label)

        self.toggle_button = Button(text="", font_size=sp(20), size_hint=(1, 0.18))
        self.toggle_button.bind(on_release=lambda *_: self.toggle())
        root.add_widget(self.toggle_button)

        self.reset_button = Button(text="Сбросить (для всех)", font_size=sp(14), size_hint=(1, 0.12))
        self.reset_button.bind(on_release=lambda *_: self.reset())
        root.add_widget(self.reset_button)

        self.peers_label = Label(text="Поиск устройств рядом...", font_size=sp(12), size_hint=(1, 0.14))
        root.add_widget(self.peers_label)

        self.update_ui()
        Clock.schedule_interval(lambda dt: self.tick(), 1.0)
        Clock.schedule_interval(lambda dt: self.send_heartbeat(), HEARTBEAT_INTERVAL)
        Clock.schedule_interval(lambda dt: self.cleanup_peers(), 2.0)

        # сразу при запуске громко спрашиваем сеть: "кто здесь есть?"
        Clock.schedule_once(lambda dt: self.net.send("hello"), 0.5)

        return root

    # ---------- состояние и диск ----------

    def load_state(self):
        if os.path.exists(self.state_file):
            try:
                with open(self.state_file, "r", encoding="utf-8") as f:
                    loaded = json.load(f)
                    st = default_state()
                    st.update(loaded)
                    return st
            except (json.JSONDecodeError, OSError):
                pass
        return default_state()

    def save_state(self):
        try:
            with open(self.state_file, "w", encoding="utf-8") as f:
                json.dump(self.state, f)
        except OSError:
            pass

    # ---------- действия пользователя ----------

    def toggle(self):
        now = time.time()
        if self.state["running"]:
            self.state["accumulated"] += now - self.state["start_time"]
            self.state["running"] = False
            self.state["start_time"] = None
        else:
            if self.state["origin_time"] is None:
                self.state["origin_time"] = now
            self.state["running"] = True
            self.state["start_time"] = now
        self.state["changed_at"] = now
        self.save_state()
        self.net.send("state", self.state)
        self.update_ui()

    def on_name_changed(self):
        new_name = self.name_input.text.strip() or "Мой секундомер"
        if new_name == self.state.get("name"):
            return
        self.state["name"] = new_name
        self.state["changed_at"] = time.time()
        self.save_state()
        self.net.send("state", self.state)

    def reset(self):
        current_name = self.state.get("name", "Мой секундомер")
        self.state = default_state()
        self.state["name"] = current_name
        self.state["changed_at"] = time.time()
        self.save_state()
        self.net.send("state", self.state)
        self.update_ui()

    # ---------- сеть: синхронизация состояния ----------

    def on_network_state(self, incoming_state):
        if not incoming_state:
            return
        if incoming_state.get("changed_at", 0) > self.state.get("changed_at", 0):
            def apply(_dt):
                self.state = {
                    "name": incoming_state.get("name", self.state.get("name", "Мой секундомер")),
                    "running": incoming_state.get("running", False),
                    "start_time": incoming_state.get("start_time"),
                    "accumulated": incoming_state.get("accumulated", 0.0),
                    "origin_time": incoming_state.get("origin_time"),
                    "changed_at": incoming_state.get("changed_at", 0.0),
                }
                self.save_state()
                if self.name_input.text != self.state["name"]:
                    self.name_input.text = self.state["name"]
                self.update_ui()
            Clock.schedule_once(apply, 0)

    # ---------- сеть: автообнаружение устройств ----------

    def on_hello(self):
        # кто-то только что появился в сети — сразу шлём ему наше состояние,
        # не дожидаясь очередного heartbeat
        self.net.send("state", self.state)

    def on_peer_seen(self, ip):
        def mark(_dt):
            self.peers[ip] = time.time()
            self.update_peers_label()
        Clock.schedule_once(mark, 0)

    def cleanup_peers(self):
        now = time.time()
        expired = [ip for ip, seen in self.peers.items() if now - seen > PEER_TIMEOUT]
        for ip in expired:
            del self.peers[ip]
        self.update_peers_label()

    def update_peers_label(self):
        # исключаем свой собственный адрес из подсчёта — грубая эвристика:
        # свои broadcast-пакеты тоже иногда прилетают самому себе через loopback
        count = len(self.peers)
        if count == 0:
            self.peers_label.text = "Устройств рядом не найдено (проверь, что Wi-Fi общий)"
        else:
            self.peers_label.text = f"Устройств рядом: {count}"

    def send_heartbeat(self):
        self.net.send("state", self.state)

    # ---------- UI ----------

    def tick(self):
        elapsed = get_elapsed_seconds(self.state)
        self.time_label.text = format_hms_short(elapsed)
        self.breakdown_label.text = format_duration_breakdown(elapsed)
        if self.state["origin_time"]:
            self.origin_label.text = "Отсчёт начат: " + format_origin_datetime(self.state["origin_time"])
        else:
            self.origin_label.text = "Отсчёт ещё не начат"

    def update_ui(self):
        self.tick()
        if self.state["running"]:
            self.toggle_button.text = "Стоп"
            self.status_label.text = "Идёт — синхронизировано по сети"
        else:
            self.toggle_button.text = "Старт"
            self.status_label.text = "Остановлен"

    def on_stop(self):
        self.net.close()


if __name__ == "__main__":
    StopwatchApp().run()
