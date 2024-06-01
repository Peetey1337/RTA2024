
from typing import Any, Iterator
from queue import Queue


SERVER: dict[str, Queue] = {}


class KafkaConsumer:

    def __init__(self, topic, **kwargs):
        self.topic = topic
        SERVER.setdefault(topic, Queue())

    def __iter__(self) -> Iterator[Any]:
        return self

    def __next__(self) -> Any:
        return SERVER[self.topic].get()


class KafkaProducer:

    def __init__(self, **kwargs):
        pass

    def send(self, topic: str, value):
        queue = SERVER[topic]
        queue.put_nowait(value)
