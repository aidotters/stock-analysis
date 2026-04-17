"""Custom exceptions for the executives module."""


class ExecutiveError(Exception):
    """経営陣分析モジュールの基底例外."""


class EdinetFetchError(ExecutiveError):
    """EDINET APIからの取得に失敗した場合の例外（リトライを尽くした後）."""


class EdinetParseError(ExecutiveError):
    """iXBRLパースに失敗した場合の例外."""


class CommunicationCollectionError(ExecutiveError):
    """WebSearchによる発信コンテンツ収集に失敗した場合の例外."""


class EvaluationError(ExecutiveError):
    """LLMによるスコア評価に失敗した場合の例外（リトライを尽くした後）."""
