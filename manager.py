from src.utils.base_manager import BaseManager
import src.translation.mapper as spm


class TranslationManager(BaseManager):
    """
    this manager class handles translation and conversion among several languages
    within Python

    """

    REGISTRY = {
        "chinese_converter": spm.ChineseConverter,
    }

    def execute(self, mapper, **kwargs):
        """
        transform a table with translation or re-transform a table converting translation among several languages.
        """
        mapper = self._load_mapper(mapper)
        mapper(self.dm).execute(**kwargs)
