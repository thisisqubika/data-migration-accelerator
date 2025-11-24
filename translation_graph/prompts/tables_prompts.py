from . import PromptBase


class TablesPrompts(PromptBase):
    """Prompts for table translation."""

    SYSTEM_TEMPLATE = "<placeholder>"

    @classmethod
    def create_prompt(cls, **kwargs):
        """Create table translation system prompt."""
        return cls.system_prompt(**kwargs)
