from . import PromptBase


class RouterPrompts(PromptBase):
    """Prompts for the smart router node."""

    SYSTEM_TEMPLATE = "<placeholder>"

    @classmethod
    def create_prompt(cls, **kwargs):
        """Create router system prompt."""
        return cls.system_prompt(**kwargs)
