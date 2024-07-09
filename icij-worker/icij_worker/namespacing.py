from abc import ABC, abstractmethod


class Namespacing(ABC):
    @abstractmethod
    def should_run_task(self, task_key: str) -> bool: ...
