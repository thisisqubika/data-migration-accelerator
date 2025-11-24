from . import PromptBase


class ViewsPrompts(PromptBase):
    """Prompts for view translation."""

    SYSTEM_TEMPLATE = "<placeholder>"

    @classmethod
    def create_prompt(cls, **kwargs):
        """Create view translation system prompt."""
        return cls.system_prompt(**kwargs)
