import asyncio
import json
import logging
import os
from pathlib import Path
from dataclasses import dataclass
from datetime import datetime
from typing import Dict, List, Optional, Tuple
from zoneinfo import ZoneInfo

import gspread
from aiogram import Bot, Dispatcher
from aiogram.filters import Command
from aiogram.types import Message
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from gspread.exceptions import WorksheetNotFound
from dotenv import load_dotenv


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
logger = logging.getLogger("tg_sheet_scheduler")


WEEKDAY_ALIASES: Dict[str, str] = {
    "mon": "mon",
    "monday": "mon",
    "пн": "mon",
    "пон": "mon",
    "tue": "tue",
    "tuesday": "tue",
    "вт": "tue",
    "ср": "wed",
    "wed": "wed",
    "wednesday": "wed",
    "чт": "thu",
    "thu": "thu",
    "thursday": "thu",
    "пт": "fri",
    "fri": "fri",
    "friday": "fri",
    "сб": "sat",
    "sat": "sat",
    "saturday": "sat",
    "вс": "sun",
    "sun": "sun",
    "sunday": "sun",
}


MEDIA_SENDERS = {
    "photo": "send_photo",
    "video": "send_video",
    "document": "send_document",
}


# Map human-friendly column headers to internal keys so the sheet can use
# readable Russian/English names interchangeably.
COLUMN_ALIASES: Dict[str, str] = {
    "enabled": "enabled",
    "вкл": "enabled",
    "активно": "enabled",
    "on": "enabled",
    "yes": "enabled",
    "chat_id": "chat_id",
    "чат": "chat_id",
    "чат id": "chat_id",
    "message": "message",
    "сообщение": "message",
    "text": "message",
    "время": "time",
    "time": "time",
    "дни": "weekday",
    "день недели": "weekday",
    "weekday": "weekday",
    "число": "monthday",
    "дата": "monthday",
    "monthday": "monthday",
    "media_type": "media_type",
    "тип медиа": "media_type",
    "media": "media_type",
    "media_url": "media_url",
    "ссылка медиа": "media_url",
    "файл": "media_url",
    "parse_mode": "parse_mode",
    "формат": "parse_mode",
}


@dataclass
class Config:
    telegram_token: str
    sheet_id: str
    worksheet: str
    service_account_json: str
    refresh_minutes: int = 5
    timezone: str = "Europe/Moscow"
    parse_mode: str = "HTML"

    @staticmethod
    def from_env() -> "Config":
        return Config(
            telegram_token=os.environ["TELEGRAM_BOT_TOKEN"],
            sheet_id=os.environ["GOOGLE_SHEET_ID"],
            worksheet=os.environ.get("GOOGLE_SHEET_WORKSHEET", "Sheet1"),
            service_account_json=os.environ.get(
                "GOOGLE_SERVICE_ACCOUNT_JSON", "service_account.json"
            ),
            refresh_minutes=int(os.environ.get("REFRESH_MINUTES", "5")),
            timezone=os.environ.get("TZ", "Europe/Moscow"),
            parse_mode=os.environ.get("DEFAULT_PARSE_MODE", "HTML"),
        )


@dataclass
class Task:
    job_id: str
    chat_id: str
    time_str: str
    weekdays: Optional[List[str]]
    monthday: Optional[int]
    message: str
    media_type: Optional[str]
    media_url: Optional[str]
    parse_mode: str


class SheetScheduler:
    def __init__(self, bot: Bot, config: Config):
        self.bot = bot
        self.config = config
        self.tz = ZoneInfo(config.timezone)
        self.scheduler = AsyncIOScheduler(timezone=self.tz)
        self.gc = gspread.service_account(filename=config.service_account_json)

    async def start(self) -> None:
        await self.refresh_jobs()
        self.scheduler.add_job(
            self._wrap_coro(self.refresh_jobs),
            "interval",
            minutes=self.config.refresh_minutes,
            id="refresh_jobs",
            replace_existing=True,
        )
        self.scheduler.start()
        logger.info("Scheduler started; jobs loaded.")

    def shutdown(self) -> None:
        self.scheduler.shutdown(wait=False)

    async def refresh_jobs(self) -> None:
        tasks = await self._read_sheet_tasks()
        desired_ids = {task.job_id for task in tasks}
        current_ids = {job.id for job in self.scheduler.get_jobs() if job.id != "refresh_jobs"}

        for job_id in current_ids - desired_ids:
            self.scheduler.remove_job(job_id)
            logger.info("Removed outdated job %s", job_id)

        for task in tasks:
            self._schedule_task(task)

        logger.info("Jobs synced: %s active", len(desired_ids))

    def _schedule_task(self, task: Task) -> None:
        hour, minute = parse_time(task.time_str)
        trigger = CronTrigger(
            hour=hour,
            minute=minute,
            day=task.monthday or "*",
            day_of_week=",".join(task.weekdays) if task.weekdays else "*",
            timezone=self.tz,
        )
        self.scheduler.add_job(
            self._wrap_coro(self._send_task, task),
            trigger=trigger,
            id=task.job_id,
            replace_existing=True,
            misfire_grace_time=300,
        )
        logger.info(
            "Scheduled job %s -> chat %s at %s (weekdays=%s monthday=%s)",
            task.job_id,
            task.chat_id,
            task.time_str,
            task.weekdays,
            task.monthday,
        )

    async def _send_task(self, task: Task) -> None:
        await send_message(
            bot=self.bot,
            chat_id=task.chat_id,
            text=task.message,
            media_type=task.media_type,
            media_url=task.media_url,
            parse_mode=task.parse_mode,
        )

    def _normalize_row(self, row: Dict[str, str]) -> Dict[str, str]:
        """Normalize sheet headers to internal keys so people can rename columns."""
        normalized: Dict[str, str] = {}
        for key, value in row.items():
            norm_key = COLUMN_ALIASES.get(str(key).strip().lower(), str(key).strip().lower())
            normalized[norm_key] = value
        return normalized

    async def _read_sheet_tasks(self) -> List[Task]:
        try:
            sh = self.gc.open_by_key(self.config.sheet_id)
            try:
                ws = sh.worksheet(self.config.worksheet)
            except WorksheetNotFound:
                ws = sh.sheet1
        except Exception as exc:
            logger.error("Failed to open sheet: %s", exc)
            return []

        try:
            data = ws.get_all_records()
        except Exception as exc:
            logger.error("Failed to read sheet rows: %s", exc)
            return []
        tasks: List[Task] = []
        for idx, row in enumerate(data, start=2):  # header is row 1
            row = self._normalize_row(row)

            enabled = str(row.get("enabled", "")).strip().lower()
            if enabled not in {"1", "true", "yes", "y", "да"}:
                continue

            chat_id = str(row.get("chat_id", "")).strip()
            message = str(row.get("message", "")).strip()
            time_str = str(row.get("time", "")).strip()
            if not chat_id or not message or not time_str:
                logger.warning("Skip row %s: missing chat_id/message/time", idx)
                continue

            try:
                parse_time(time_str)
                weekdays = parse_weekdays(row.get("weekday"))
                monthday = parse_monthday(row.get("monthday"))
                media_type, media_url = parse_media(row.get("media_type"), row.get("media_url"))
                parse_mode = str(row.get("parse_mode") or self.config.parse_mode).strip() or "HTML"
            except ValueError as exc:
                logger.warning("Skip row %s: %s", idx, exc)
                continue

            job_id = f"row{idx}-{chat_id}-{time_str}-{weekdays or 'any'}-{monthday or 'any'}"
            tasks.append(
                Task(
                    job_id=job_id,
                    chat_id=chat_id,
                    time_str=time_str,
                    weekdays=weekdays,
                    monthday=monthday,
                    message=message,
                    media_type=media_type,
                    media_url=media_url,
                    parse_mode=parse_mode,
                )
            )
        return tasks

    @staticmethod
    def _wrap_coro(coro_func, *args, **kwargs):
        async def runner():
            try:
                await coro_func(*args, **kwargs)
            except Exception as exc:
                logger.exception("Job failed: %s", exc)

        return runner


def parse_time(value: str) -> Tuple[int, int]:
    parts = value.strip().split(":")
    if len(parts) != 2:
        raise ValueError("Time must be HH:MM")
    hour, minute = int(parts[0]), int(parts[1])
    if not (0 <= hour <= 23 and 0 <= minute <= 59):
        raise ValueError("Time out of range")
    return hour, minute


def parse_weekdays(value) -> Optional[List[str]]:
    if value is None or str(value).strip() == "":
        return None
    tokens = str(value).replace(";", ",").split(",")
    result = []
    for token in tokens:
        key = token.strip().lower()
        if not key:
            continue
        mapped = WEEKDAY_ALIASES.get(key)
        if not mapped:
            raise ValueError(f"Unknown weekday: {token}")
        result.append(mapped)
    return result or None


def parse_monthday(value) -> Optional[int]:
    if value is None or str(value).strip() == "":
        return None
    md = int(value)
    if not 1 <= md <= 31:
        raise ValueError("monthday must be 1-31")
    return md


def parse_media(media_type, media_url) -> Tuple[Optional[str], Optional[str]]:
    mtype = str(media_type or "").strip().lower()
    url = str(media_url or "").strip()
    if not mtype:
        return None, None
    if mtype not in MEDIA_SENDERS:
        raise ValueError(f"Unsupported media_type {mtype}")
    if not url:
        raise ValueError("media_url required when media_type set")
    return mtype, url


async def send_message(
    bot: Bot,
    chat_id: str,
    text: str,
    media_type: Optional[str],
    media_url: Optional[str],
    parse_mode: str,
) -> None:
    attempts = 3
    for attempt in range(1, attempts + 1):
        try:
            if media_type and media_url:
                sender_name = MEDIA_SENDERS[media_type]
                sender = getattr(bot, sender_name)
                await sender(chat_id=chat_id, caption=text, parse_mode=parse_mode, **{media_type: media_url})
            else:
                await bot.send_message(chat_id=chat_id, text=text, parse_mode=parse_mode, disable_web_page_preview=True)
            return
        except Exception as exc:
            if attempt == attempts:
                logger.exception("Failed to send to %s after %s attempts: %s", chat_id, attempt, exc)
                return
            sleep_for = attempt * 2
            logger.warning("Send failed (attempt %s/%s), retry in %ss: %s", attempt, attempts, sleep_for, exc)
            await asyncio.sleep(sleep_for)


async def main() -> None:
    load_dotenv(dotenv_path=Path(".env"))
    config = Config.from_env()
    logger.info("Starting bot scheduler for sheet %s", config.sheet_id)

    async with Bot(token=config.telegram_token) as bot:
        dp = Dispatcher()

        @dp.message(Command("chat_id"))
        async def chat_id_handler(message: Message) -> None:
            await message.reply(f"chat_id: {message.chat.id}")

        scheduler = SheetScheduler(bot=bot, config=config)
        await scheduler.start()
        try:
            await dp.start_polling(bot)
        finally:
            scheduler.shutdown()
            logger.info("Shutdown complete.")


if __name__ == "__main__":
    asyncio.run(main())

