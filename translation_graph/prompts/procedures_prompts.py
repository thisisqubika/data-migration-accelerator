from . import PromptBase


class ProceduresPrompts(PromptBase):
    """Prompts for procedure translation."""

    SYSTEM_TEMPLATE = "<placeholder>"

    @classmethod
    def create_prompt(cls, **kwargs):
        """Create procedure translation system prompt."""
        return cls.system_prompt(**kwargs)
