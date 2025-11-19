# src/events/event_bus.py
import threading
import queue
import time
from typing import Callable, Any, Dict, List

class Event:
    def __init__(self, name: str, payload: Any = None):
        self.name = name
        self.payload = payload
        self.timestamp = time.time()

class EventBus:
    """
    Simple in-process pub/sub event bus.
    Subscribers receive (event_name, payload).
    Each subscriber runs in its own worker thread.
    """

    def __init__(self):
        self._subscribers: Dict[str, List[Callable[[Event], None]]] = {}
        self._lock = threading.Lock()

    def subscribe(self, event_name: str, handler: Callable[[Event], None]):
        """
        Subscribe a handler to an event name.
        Handler signature: handler(Event)
        """
        with self._lock:
            self._subscribers.setdefault(event_name, []).append(handler)

    def unsubscribe(self, event_name: str, handler: Callable[[Event], None]):
        with self._lock:
            if event_name in self._subscribers:
                try:
                    self._subscribers[event_name].remove(handler)
                except ValueError:
                    pass

    def publish(self, event_name: str, payload: Any = None):
        """
        Publish an event. Handlers are executed in separate threads (fire-and-forget).
        """
        with self._lock:
            handlers = list(self._subscribers.get(event_name, []))

        event = Event(event_name, payload)
        for h in handlers:
            threading.Thread(target=self._safe_call, args=(h, event), daemon=True).start()

    def _safe_call(self, handler: Callable[[Event], None], event: Event):
        try:
            handler(event)
        except Exception as e:
            # Minimal logging; you can replace with structured logger
            print(f"⚠️ Event handler error for {event.name}: {e}")
