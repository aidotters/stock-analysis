"""Custom exceptions for the Notion export pipeline."""

from __future__ import annotations


class NotionExportError(Exception):
    """Base exception for all notion_export errors."""


class ReportDirectoryNotFoundError(NotionExportError):
    """Raised when the requested report directory does not exist."""


class ParentPageNotFoundError(NotionExportError):
    """Raised when NOTION_PARENT_PAGE_ID does not resolve to a Notion page."""


class TooManyExistingPagesError(NotionExportError):
    """Raised when 3+ existing pages with the same code are found."""

    def __init__(self, count: int, page_ids: list[str]) -> None:
        super().__init__(
            f"アーカイブ候補が{count}件見つかりました。手動で整理してください。"
        )
        self.count = count
        self.page_ids = list(page_ids)


class NotionApiError(NotionExportError):
    """Raised when the Notion REST API returns an unsuccessful response."""

    def __init__(self, status_code: int, body: str) -> None:
        super().__init__(f"Notion API error: status={status_code} body={body[:200]}")
        self.status_code = status_code
        self.body = body


class ImageUploadError(NotionExportError):
    """Raised internally by ImageUploader when an upload fails non-fatally."""
