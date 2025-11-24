from abc import ABC, abstractmethod
from typing import Any


class PromptBase(ABC):
    SYSTEM_TEMPLATE: str = "<placeholder system template>"

    @classmethod
    @abstractmethod
    def create_prompt(cls, **kwargs) -> Any:
        pass

    @classmethod
    def system_prompt(cls, **kwargs):
        return cls.SYSTEM_TEMPLATE.format(**kwargs)
