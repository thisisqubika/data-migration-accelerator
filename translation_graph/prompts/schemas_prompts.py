from . import PromptBase


class SchemasPrompts(PromptBase):
    """Prompts for schema translation."""

    SYSTEM_TEMPLATE = "<placeholder>"

    @classmethod
    def create_prompt(cls, **kwargs):
        """Create schema translation system prompt."""
        return cls.system_prompt(**kwargs)
