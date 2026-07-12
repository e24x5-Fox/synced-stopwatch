# -*- coding: utf-8 -*-
"""
Синхронизированный секундомер (Kivy + UDP broadcast).

Идея:
- У каждого запущенного экземпляра (телефон/комп) есть локальное состояние:
  running (идёт ли отсчёт), start_time (когда стартовали, абсолютное unix-время),
  accumulated (сколько секунд накоплено до этого старта), changed_at (момент
  последнего изменения состояния — используется для разрешения конфликтов).
- Состояние сохраняется на диск (переживает перезапуск приложения и
  перезагрузку устройства — секундомер продолжит считать правильно).
- По локальной сети (Wi-Fi) состояние рассылается UDP-широковещанием
  (broadcast) на порт 5005:
    - сразу же при нажатии Старт/Стоп/Сброс,
    - и раз в 3 секунды "на всякий случай" (heartbeat), чтобы новые
      устройства, зашедшие в сеть, сразу подхватили текущее состояние,
      а потерянные из-за Wi-Fi пакеты не ломали синхронизацию надолго.
- Правило разрешения конфликтов простое: "побеждает" то состояние, у
  которого changed_at больше (последнее по времени действие). Это работает
  надёжно, если часы устройств не разъезжаются на секунды — для домашней
  сети этого обычно достаточно.

Ограничение: broadcast работает только в пределах одной локальной сети
(один Wi-Fi/один роутер). Если на роутере включена "изоляция клиентов"
(AP isolation), устройства не увидят друг друга — это настройка роутера,
не приложения.
"""

import json
import os
import socket
import threading
import time

from kivy.app import App
from kivy.clock import Clock
from kivy.uix.boxlayout import BoxLayout
from kivy.uix.label import Label
from kivy.uix.button import Button
from kivy.metrics import sp

UDP_PORT = 5005
BROADCAST_ADDR = "255.255.255.255"
HEARTBEAT_INTERVAL = 3.0

try:
    from kivy.app import App as _App
    APP_DATA_DIR = None  # заполним в build(), user_data_dir доступен только у экземпляра приложения
except Exception:
    APP_DATA_DIR = None


def default_state():
    return {
        "running": False,
        "start_time": None,
        "accumulated": 0.0,
        "changed_at": 0.0,
    }


def get_elapsed_seconds(state):
    if state["running"] and state["start_time"] is not None:
        return state["accumulated"] + (time.time() - state["start_time"])
    return state["accumulated"]


def format_hms(total_seconds):
    total_seconds = max(0, int(total_seconds))
    days, rem = divmod(total_seconds, 86400)
    hours, rem = divmod(rem, 3600)
    minutes, seconds = divmod(rem, 60)
    if days > 0:
        return f"{days} дн. {hours:02d}:{minutes:02d}:{seconds:02d}"
    return f"{hours:02d}:{minutes:02d}:{seconds:02d}"


class NetworkSync:
    """UDP-приём и рассылка состояния секундомера по локальной сети."""

    def __init__(self, on_state_received):
        self.on_state_received = on_state_received
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        try:
            self.sock.bind(("", UDP_PORT))
        except OSError:
            # порт занят другим процессом на этом же устройстве — редкий случай
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
                if msg.get("type") == "stopwatch_state":
                    self.on_state_received(msg["state"])
            except (json.JSONDecodeError, KeyError):
                pass

    def broadcast(self, state):
        payload = json.dumps({"type": "stopwatch_state", "state": state}).encode("utf-8")
        try:
            self.sock.sendto(payload, (BROADCAST_ADDR, UDP_PORT))
        except OSError:
            pass  # нет сети — просто не отправляем, локально всё равно работает

    def close(self):
        self.running = False
        self.sock.close()


class RootLayout(BoxLayout):
    pass


class StopwatchApp(App):
    def build(self):
        self.state_file = os.path.join(self.user_data_dir, "stopwatch_state.json")
        os.makedirs(self.user_data_dir, exist_ok=True)
        self.state = self.load_state()

        self.net = NetworkSync(self.on_network_state)

        root = BoxLayout(orientation="vertical", padding=sp(24), spacing=sp(16))

        self.time_label = Label(text="00:00:00", font_size=sp(48), bold=True, size_hint=(1, 0.4))
        root.add_widget(self.time_label)

        self.status_label = Label(text="", font_size=sp(16), size_hint=(1, 0.15))
        root.add_widget(self.status_label)

        self.toggle_button = Button(text="", font_size=sp(20), size_hint=(1, 0.2))
        self.toggle_button.bind(on_release=lambda *_: self.toggle())
        root.add_widget(self.toggle_button)

        self.reset_button = Button(text="Сбросить (для всех)", font_size=sp(16), size_hint=(1, 0.15))
        self.reset_button.bind(on_release=lambda *_: self.reset())
        root.add_widget(self.reset_button)

        self.peers_label = Label(text="Ожидание других устройств...", font_size=sp(12), size_hint=(1, 0.1))
        root.add_widget(self.peers_label)

        self.update_ui()
        Clock.schedule_interval(lambda dt: self.tick(), 1.0)
        Clock.schedule_interval(lambda dt: self.send_heartbeat(), HEARTBEAT_INTERVAL)
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
            self.state["running"] = True
            self.state["start_time"] = now
        self.state["changed_at"] = now
        self.save_state()
        self.net.broadcast(self.state)
        self.update_ui()

    def reset(self):
        self.state = default_state()
        self.state["changed_at"] = time.time()
        self.save_state()
        self.net.broadcast(self.state)
        self.update_ui()

    # ---------- сеть ----------

    def on_network_state(self, incoming_state):
        # last-writer-wins: применяем чужое состояние, только если оно "свежее"
        if incoming_state.get("changed_at", 0) > self.state.get("changed_at", 0):
            def apply(_dt):
                self.state = {
                    "running": incoming_state["running"],
                    "start_time": incoming_state["start_time"],
                    "accumulated": incoming_state["accumulated"],
                    "changed_at": incoming_state["changed_at"],
                }
                self.save_state()
                self.update_ui()
                self.peers_label.text = "Синхронизировано с другим устройством"
            Clock.schedule_once(apply, 0)

    def send_heartbeat(self):
        self.net.broadcast(self.state)

    # ---------- UI ----------

    def tick(self):
        elapsed = get_elapsed_seconds(self.state)
        self.time_label.text = format_hms(elapsed)

    def update_ui(self):
        self.tick()
        if self.state["running"]:
            self.toggle_button.text = "Стоп"
            self.status_label.text = "Идёт — синхронизируется по Wi-Fi со всеми устройствами"
        else:
            self.toggle_button.text = "Старт"
            self.status_label.text = "Остановлен"

    def on_stop(self):
        self.net.close()


if __name__ == "__main__":
    StopwatchApp().run()
