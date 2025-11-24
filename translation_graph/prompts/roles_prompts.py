from . import PromptBase


class RolesPrompts(PromptBase):
    """Prompts for role translation."""

    SYSTEM_TEMPLATE = "<placeholder>"

    @classmethod
    def create_prompt(cls, **kwargs):
        """Create role translation system prompt."""
        return cls.system_prompt(**kwargs)
