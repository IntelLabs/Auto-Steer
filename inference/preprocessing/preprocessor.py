"""Base clase for query plan preprocessors"""


class QueryPlanPreprocessor:
    """Defines the basic interface for plan preprocessors"""

    def __init__(self):
        pass

    def fit(self, trees) -> None:
        raise NotImplementedError()

    def transform(self, trees) -> list:
        """Return a list of feature vectors for the provided trees"""
        raise NotImplementedError()
