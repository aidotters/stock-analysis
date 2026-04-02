"""
Slack notification module for cron job results.

Provides SlackNotifier for sending notifications and JobContext
as a context manager for automatic success/error reporting.
"""

import logging
import time
import traceback
from dataclasses import dataclass, field
from datetime import datetime
from types import TracebackType
from typing import Optional

import requests

from market_pipeline.config import get_settings

logger = logging.getLogger(__name__)


@dataclass
class JobResult:
    """Data class holding job execution results."""

    job_name: str
    success: bool = True
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    metrics: dict[str, str] = field(default_factory=dict)
    errors: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)

    @property
    def duration_formatted(self) -> str:
        """Return duration as 'X分Y秒' formatted string."""
        if self.start_time is None or self.end_time is None:
            return "不明"
        delta = self.end_time - self.start_time
        total_seconds = int(delta.total_seconds())
        minutes = total_seconds // 60
        seconds = total_seconds % 60
        if minutes > 0:
            return f"{minutes}分{seconds}秒"
        return f"{seconds}秒"


class SlackNotifier:
    """Sends notifications to Slack via Incoming Webhook."""

    def __init__(self) -> None:
        settings = get_settings()
        self._webhook_url = settings.slack.webhook_url
        self._error_webhook_url = settings.slack.error_webhook_url
        self._enabled = settings.slack.enabled
        self._timeout = settings.slack.timeout_seconds
        self._max_retries = settings.slack.max_retries

    @property
    def is_available(self) -> bool:
        """Return True if Slack notifications can be sent."""
        return bool(self._webhook_url) and self._enabled

    def send_success(self, job_result: JobResult) -> None:
        """Send a success notification."""
        if not self.is_available:
            logger.info(
                "Slack通知スキップ（webhook_url=%s, enabled=%s）",
                "設定済み" if self._webhook_url else "未設定",
                self._enabled,
            )
            return

        blocks = [
            f"✅ *{job_result.job_name}* 完了",
            f"実行時間: {job_result.duration_formatted}",
        ]

        for key, value in job_result.metrics.items():
            blocks.append(f"{key}: {value}")

        if job_result.warnings:
            blocks.append("")
            blocks.append("⚠️ 警告:")
            for w in job_result.warnings:
                blocks.append(f"  • {w}")

        text = "\n".join(blocks)
        self._post(self._webhook_url, text)
        logger.info("Slack成功通知送信完了: %s", job_result.job_name)

    def send_error(self, job_result: JobResult) -> None:
        """Send an error notification with traceback info."""
        if not self.is_available:
            logger.info(
                "Slack通知スキップ（webhook_url=%s, enabled=%s）",
                "設定済み" if self._webhook_url else "未設定",
                self._enabled,
            )
            return

        blocks = [
            f"❌ *{job_result.job_name}* 失敗",
            f"実行時間: {job_result.duration_formatted}",
        ]

        if job_result.errors:
            blocks.append("")
            blocks.append("エラー内容:")
            for err in job_result.errors:
                blocks.append(f"```{err}```")

        text = "\n".join(blocks)
        url = self._error_webhook_url if self._error_webhook_url else self._webhook_url
        self._post(url, text)
        logger.info("Slackエラー通知送信完了: %s", job_result.job_name)

    def send_warning(self, job_name: str, message: str, details: str = "") -> None:
        """Send a warning notification."""
        if not self.is_available:
            logger.info(
                "Slack通知スキップ（webhook_url=%s, enabled=%s）",
                "設定済み" if self._webhook_url else "未設定",
                self._enabled,
            )
            return

        blocks = [f"⚠️ *{job_name}* 警告", message]
        if details:
            blocks.append(f"```{details}```")

        text = "\n".join(blocks)
        self._post(self._webhook_url, text)
        logger.info("Slack警告通知送信完了: %s", job_name)

    def _post(self, url: str, text: str) -> None:
        """Post a message to Slack with retry logic."""
        payload = {"text": text}
        last_exc: Optional[Exception] = None

        for attempt in range(self._max_retries):
            try:
                resp = requests.post(url, json=payload, timeout=self._timeout)
                resp.raise_for_status()
                logger.info("Slack Webhook送信成功 (status=%d)", resp.status_code)
                return
            except Exception as e:
                last_exc = e
                logger.warning(
                    "Slack通知送信失敗 (試行 %d/%d): %s",
                    attempt + 1,
                    self._max_retries,
                    e,
                )
                if attempt < self._max_retries - 1:
                    time.sleep(1)

        logger.error(
            "Slack通知の送信に失敗しました（全%dリトライ失敗）: %s",
            self._max_retries,
            last_exc,
        )


class JobContext:
    """Context manager that wraps a job and sends Slack notifications."""

    def __init__(self, job_name: str) -> None:
        self._job_result = JobResult(job_name=job_name)
        self._notifier = SlackNotifier()

    def __enter__(self) -> "JobContext":
        self._job_result.start_time = datetime.now()
        return self

    def __exit__(
        self,
        exc_type: Optional[type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[TracebackType],
    ) -> None:
        self._job_result.end_time = datetime.now()

        try:
            if exc_type is not None:
                self._job_result.success = False
                tb_str = "".join(traceback.format_exception(exc_type, exc_val, exc_tb))
                self._job_result.errors.append(str(exc_val))
                self._job_result.errors.append(tb_str)
                self._notifier.send_error(self._job_result)
            else:
                self._job_result.success = True
                self._notifier.send_success(self._job_result)
        except Exception:
            logger.warning("Slack通知の送信中にエラーが発生しました", exc_info=True)

    def add_metric(self, key: str, value: str) -> None:
        """Add a metric to be included in the notification."""
        self._job_result.metrics[key] = value

    def add_warning(self, message: str) -> None:
        """Add a warning to be included in the notification."""
        self._job_result.warnings.append(message)
