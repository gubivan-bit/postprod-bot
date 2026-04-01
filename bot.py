"""
🎬 PostProd Task Bot — v2.1
Telegram + GPT-4o + Notion

Возможности:
  • Создаёт задачи из сообщений с #хэштегом (выпуск/серия)
  • Транскрибирует голосовые через Whisper
  • Проверяет: обязателен ли #хэштег в этой ветке
  • Автоматически прикрепляет ссылки (Sheets, Яндекс.Диск, Drive, Frame.io)
  • Кнопки подтверждения перед созданием задачи
  • GPT-4o анализирует контекст и паттерны прошлых проектов
  • Счётчик итераций правок (бесконечный)
  • Следит за дедлайнами и ожиданием внешних отделов
  • Утренний дайджест в 9:00
  • Дневник активности + ежедневный GPT-отчёт

Новый UX (v2.1) — без reply на первое сообщение:
  /task авито 25            → карточка с кнопками статуса
  /авито 25                 → то же самое (умная команда)
  /s авито 25 монтаж        → смена статуса по проекту+выпуску

Статусы через reply на сообщение задачи:
  /inprogress   ✂️ Монтаж
  /review       🔄 Правки (+1 итерация)
  /final        👁️ Финальный просмотр
  /done         ✅ Сдано
  /freeze       ⏸️ Заморожено

Управление (reply на задачу):
  /deadline     📅 Установить дедлайн
  /assign       👤 Назначить исполнителя
  /waiting      🏢 Ждём внешний отдел (цвет/звук/графику)
  /report       📊 Дайджест всех задач
  /help         📖 Справка
"""

import asyncio
import io
import json
import logging
import os
import random
import re
import string
from datetime import date, datetime, time, timedelta
from typing import Optional

from notion_client import Client as NotionClient
from openai import AsyncOpenAI
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.ext import (
    Application,
    CallbackQueryHandler,
    ChatMemberHandler,
    CommandHandler,
    ContextTypes,
    JobQueue,
    MessageHandler,
    filters,
)

# ─────────────────────────────────────────────────────────────────────────────
# Конфигурация (из переменных окружения)
# ─────────────────────────────────────────────────────────────────────────────
TELEGRAM_TOKEN   = os.environ["TELEGRAM_TOKEN"]
OPENAI_API_KEY   = os.environ["OPENAI_API_KEY"]
NOTION_TOKEN     = os.environ["NOTION_TOKEN"]

# Notion database IDs (уже созданы — не менять!)
TASKS_DB    = os.environ.get("NOTION_TASKS_DB",    "f9f3a6736e81473e8a64ff6000833119")
PROJECTS_DB = os.environ.get("NOTION_PROJECTS_DB", "7e7b1e5c0323430cac6bc22c8f26c875")
PATTERNS_DB = os.environ.get("NOTION_PATTERNS_DB", "01f7f279ef1442bb83f95c114bde90b0")
DIARY_DB    = os.environ.get("NOTION_DIARY_DB",    "f0aa66009b8e4ef2b9f98d4123c94155")
REPORTS_DB  = os.environ.get("NOTION_REPORTS_DB",  "b2d4a56277bb44efb605895f5f7a4387")

# Чаты куда слать дайджест/алерты (через запятую, например: -100123456,-100654321)
DIGEST_CHAT_IDS: list[int] = [
    int(x.strip())
    for x in os.environ.get("DIGEST_CHAT_IDS", "").split(",")
    if x.strip()
]

DIGEST_HOUR      = int(os.environ.get("DIGEST_HOUR", "9"))
WAITING_ALERT_DAYS = int(os.environ.get("WAITING_ALERT_DAYS", "2"))

# Минимальный порог уверенности GPT для показа кнопок подтверждения
GPT_CONFIDENCE_THRESHOLD = 0.55

notion = NotionClient(auth=NOTION_TOKEN)
openai_client = AsyncOpenAI(api_key=OPENAI_API_KEY)

logging.basicConfig(
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# Определение ссылок
# ─────────────────────────────────────────────────────────────────────────────
_FILE_LINK_RE = re.compile(
    r"docs\.google\.com/spreadsheets"
    r"|docs\.google\.com/document"
    r"|drive\.google\.com"
    r"|disk\.yandex\.(ru|com)"
    r"|frame\.io"
    r"|app\.frame\.io"
    r"|wetransfer\.com"
    r"|dropbox\.com"
    r"|vimeo\.com",
    re.IGNORECASE,
)
_URL_RE = re.compile(r"https?://[^\s<>\"{}|\\^`\[\]]+")


def extract_urls(text: str) -> list[str]:
    return _URL_RE.findall(text)


def is_file_link(url: str) -> bool:
    return bool(_FILE_LINK_RE.search(url))


# ─────────────────────────────────────────────────────────────────────────────
# Парсинг текста
# ─────────────────────────────────────────────────────────────────────────────
def escape_md(text: str) -> str:
    """Экранирует спецсимволы Markdown v1 в динамическом тексте."""
    for ch in ("_", "*", "`", "["):
        text = text.replace(ch, "\\" + ch)
    return text


def _short_key(n: int = 8) -> str:
    """Генерирует короткий случайный ключ для хранения в bot_data."""
    return "".join(random.choices(string.ascii_lowercase + string.digits, k=n))


def parse_hashtags(text: str) -> list[str]:
    # Поддержка точек в номерах выпусков: #ep2.1, #2.2, #серия_1.3
    return re.findall(r"#([\w][\w.]*)", text)


def parse_mentions(text: str) -> list[str]:
    return re.findall(r"@(\w+)", text)


def build_tg_link(chat_id: int, message_id: int) -> str:
    """Собирает прямую ссылку на сообщение в Telegram."""
    clean = str(abs(chat_id))
    if clean.startswith("100"):
        clean = clean[3:]
    return f"https://t.me/c/{clean}/{message_id}"


def parse_date_input(text: str) -> Optional[str]:
    """Парсит дату в произвольном формате. Возвращает ISO-строку или None."""
    text = text.strip().lower()
    today = date.today()

    if "завтра" in text:
        return (today + timedelta(days=1)).isoformat()
    if "послезавтра" in text:
        return (today + timedelta(days=2)).isoformat()

    m = re.match(r"(\d{1,2})[.\-/](\d{1,2})(?:[.\-/](\d{2,4}))?", text)
    if m:
        day, month = int(m.group(1)), int(m.group(2))
        year = int(m.group(3)) if m.group(3) else today.year
        if year < 100:
            year += 2000
        try:
            return date(year, month, day).isoformat()
        except ValueError:
            pass

    months = {
        "января": 1, "февраля": 2, "марта": 3, "апреля": 4,
        "мая": 5, "июня": 6, "июля": 7, "августа": 8,
        "сентября": 9, "октября": 10, "ноября": 11, "декабря": 12,
    }
    for name, num in months.items():
        if name in text:
            m2 = re.search(r"(\d{1,2})", text)
            if m2:
                try:
                    return date(today.year, num, int(m2.group(1))).isoformat()
                except ValueError:
                    pass
    return None


# ─────────────────────────────────────────────────────────────────────────────
# Notion helpers
# ─────────────────────────────────────────────────────────────────────────────
def _rich_text(text: str) -> list:
    return [{"text": {"content": text[:2000]}}]


def _get_rich(props: dict, key: str) -> str:
    parts = props.get(key, {}).get("rich_text", [])
    return parts[0]["text"]["content"] if parts else ""


def _get_select(props: dict, key: str) -> str:
    sel = props.get(key, {}).get("select")
    return sel["name"] if sel else ""


def _get_title(props: dict, key: str = "Задача") -> str:
    parts = props.get(key, {}).get("title", [])
    return parts[0]["text"]["content"] if parts else ""


def get_project_name(thread_id: int) -> Optional[str]:
    """Ищет проект в Notion по числовому ID топика Telegram."""
    try:
        res = notion.databases.query(
            database_id=PROJECTS_DB,
            filter={"property": "Топик ID", "number": {"equals": thread_id}},
            page_size=1,
        )
        pages = res.get("results", [])
        if not pages:
            return None
        return _get_title(pages[0]["properties"], "Проект")
    except Exception as e:
        logger.error(f"get_project_name: {e}")
        return None


def find_project_fuzzy(name_query: str) -> Optional[str]:
    """
    Нечёткий поиск проекта по имени (case-insensitive contains).
    Возвращает точное название проекта из Notion или None.
    """
    try:
        res = notion.databases.query(database_id=PROJECTS_DB, page_size=100)
        query_lower = name_query.lower().strip()
        best: Optional[str] = None
        for page in res.get("results", []):
            name = _get_title(page["properties"], "Проект")
            if not name:
                continue
            # Точное совпадение — сразу возвращаем
            if name.lower() == query_lower:
                return name
            # Частичное совпадение — запоминаем как кандидата
            if query_lower in name.lower() and best is None:
                best = name
        return best
    except Exception as e:
        logger.error(f"find_project_fuzzy: {e}")
        return None


def project_uses_hashtags(topic_name: str) -> bool:
    """Проверяет, включён ли режим обязательных #хэштегов для этой ветки."""
    try:
        res = notion.databases.query(
            database_id=PROJECTS_DB,
            filter={"property": "Проект", "title": {"equals": topic_name}},
            page_size=1,
        )
        pages = res.get("results", [])
        if not pages:
            return False
        sel = _get_select(pages[0]["properties"], "Использует хэштеги")
        return sel == "✅ Да"
    except Exception as e:
        logger.error(f"project_uses_hashtags: {e}")
        return False


def find_task_by_tg_link(tg_link: str) -> Optional[str]:
    try:
        res = notion.databases.query(
            database_id=TASKS_DB,
            filter={"property": "Ссылка TG", "url": {"equals": tg_link}},
            page_size=1,
        )
        pages = res.get("results", [])
        return pages[0]["id"] if pages else None
    except Exception as e:
        logger.error(f"find_task_by_tg_link: {e}")
        return None


def find_active_task(project: str, episode: str) -> Optional[dict]:
    """Последняя активная задача для проекта+выпуска."""
    try:
        filters_list = [
            {"property": "Проект", "select": {"equals": project}},
            {"property": "Статус", "select": {"does_not_equal": "✅ Сдано"}},
            {"property": "Статус", "select": {"does_not_equal": "❌ Отменено"}},
        ]
        if episode:
            filters_list.append({"property": "Выпуск", "select": {"equals": episode}})

        res = notion.databases.query(
            database_id=TASKS_DB,
            filter={"and": filters_list},
            sorts=[{"timestamp": "last_edited_time", "direction": "descending"}],
            page_size=1,
        )
        pages = res.get("results", [])
        return pages[0] if pages else None
    except Exception as e:
        logger.error(f"find_active_task: {e}")
        return None


def create_task(
    task_name: str,
    project: str,
    episode: str,
    assignees: list[str],
    tg_link: str,
    topic: str,
    status: str = "🆕 Новая",
    deadline: Optional[str] = None,
    links: Optional[list[str]] = None,
    ai_summary: str = "",
    chat_id: Optional[int] = None,
    thread_id: Optional[int] = None,
) -> Optional[str]:
    try:
        props: dict = {
            "Задача":      {"title": _rich_text(task_name[:200])},
            "Проект":      {"select": {"name": project or "Не указан"}},
            "Выпуск":      {"select": {"name": episode or "—"}},
            "Статус":      {"select": {"name": status}},
            "Исполнитель": {"rich_text": _rich_text(", ".join(f"@{a}" for a in assignees) if assignees else "—")},
            "Ссылка TG":   {"url": tg_link},
            "Топик TG":    {"rich_text": _rich_text(topic)},
            "Итерация правок": {"number": 0},
        }
        if chat_id:
            props["TG Chat ID"] = {"number": chat_id}
        if thread_id:
            props["TG Thread ID"] = {"number": thread_id}
        if deadline:
            props["Дедлайн"] = {"date": {"start": deadline}}
        if links:
            props["Ссылки"] = {"rich_text": _rich_text("\n".join(links))}
        if ai_summary:
            props["AI Сводка"] = {"rich_text": _rich_text(ai_summary)}

        page = notion.pages.create(parent={"database_id": TASKS_DB}, properties=props)
        return page["id"]
    except Exception as e:
        logger.error(f"create_task: {e}")
        return None


def update_status(page_id: str, status: str, increment_iter: bool = False) -> bool:
    try:
        props: dict = {"Статус": {"select": {"name": status}}}
        if increment_iter:
            page = notion.pages.retrieve(page_id=page_id)
            cur = page["properties"].get("Итерация правок", {}).get("number") or 0
            props["Итерация правок"] = {"number": cur + 1}
        # При переводе в Сдано — ставим флаг Архив (если поле существует в базе)
        if status == "✅ Сдано":
            try:
                props["Архив"] = {"checkbox": True}
            except Exception:
                pass  # поле может отсутствовать — не критично
        notion.pages.update(page_id=page_id, properties=props)
        return True
    except Exception as e:
        logger.error(f"update_status: {e}")
        return False


def update_deadline(page_id: str, deadline_iso: str) -> bool:
    try:
        notion.pages.update(
            page_id=page_id,
            properties={"Дедлайн": {"date": {"start": deadline_iso}}},
        )
        return True
    except Exception as e:
        logger.error(f"update_deadline: {e}")
        return False


def update_assignee(page_id: str, assignee: str) -> bool:
    try:
        notion.pages.update(
            page_id=page_id,
            properties={"Исполнитель": {"rich_text": _rich_text(f"@{assignee}")}},
        )
        return True
    except Exception as e:
        logger.error(f"update_assignee: {e}")
        return False


def set_waiting(page_id: str, dept_raw: str) -> bool:
    """Переводит задачу в статус ожидания внешнего отдела."""
    dept_map = {
        "цвет":    ("🎨 Ждём цвет",    "🎨 Цвет"),
        "звук":    ("🔊 Ждём звук",    "🔊 Звук"),
        "графику": ("✨ Ждём графику", "✨ Графика"),
        "графика": ("✨ Ждём графику", "✨ Графика"),
    }
    key = dept_raw.lower()
    if key not in dept_map:
        return False
    status_val, dept_val = dept_map[key]
    try:
        notion.pages.update(
            page_id=page_id,
            properties={
                "Статус":       {"select": {"name": status_val}},
                "Ожидаем отдел":{"select": {"name": dept_val}},
                "Ожидание с":   {"date": {"start": date.today().isoformat()}},
            },
        )
        return True
    except Exception as e:
        logger.error(f"set_waiting: {e}")
        return False


def append_links(page_id: str, new_links: list[str]) -> bool:
    try:
        page = notion.pages.retrieve(page_id=page_id)
        existing = _get_rich(page["properties"], "Ссылки")
        combined = (existing.strip() + "\n" + "\n".join(new_links)).strip()
        notion.pages.update(
            page_id=page_id,
            properties={"Ссылки": {"rich_text": _rich_text(combined)}},
        )
        return True
    except Exception as e:
        logger.error(f"append_links: {e}")
        return False


def get_all_chat_ids_from_tasks() -> set[int]:
    """
    Собирает все уникальные TG Chat ID из базы Задач.
    Используется как persistent fallback — данные выживают после редеплоя.
    """
    try:
        res = notion.databases.query(
            database_id=TASKS_DB,
            filter={
                "and": [
                    {"property": "Статус", "select": {"does_not_equal": "❌ Отменено"}},
                    {"property": "Статус", "select": {"does_not_equal": "✅ Сдано"}},
                ]
            },
            page_size=100,
        )
        ids: set[int] = set()
        for page in res.get("results", []):
            cid = (page["properties"].get("TG Chat ID") or {}).get("number")
            if cid:
                ids.add(int(cid))
        return ids
    except Exception as e:
        logger.error(f"get_all_chat_ids_from_tasks: {e}")
        return set()


def _all_digest_chats(bot_data: dict) -> list[int]:
    """
    Объединяет все источники chat_id для дайджестов:
    1. DIGEST_CHAT_IDS из env (статические, заданы вручную)
    2. registered_chats из bot_data (добавляются при входе бота в чат)
    3. Chat ID из активных задач в Notion (persistent fallback после редеплоя)
    """
    combined: set[int] = set(DIGEST_CHAT_IDS)
    combined.update(bot_data.get("registered_chats", set()))
    combined.update(get_all_chat_ids_from_tasks())
    return list(combined)


def get_tasks_by_status(status: str) -> list[dict]:
    try:
        res = notion.databases.query(
            database_id=TASKS_DB,
            filter={"property": "Статус", "select": {"equals": status}},
            sorts=[{"property": "Проект", "direction": "ascending"}],
        )
        return res.get("results", [])
    except Exception as e:
        logger.error(f"get_tasks_by_status: {e}")
        return []


def get_overdue() -> list[dict]:
    try:
        res = notion.databases.query(
            database_id=TASKS_DB,
            filter={
                "and": [
                    {"property": "Дедлайн", "date": {"before": date.today().isoformat()}},
                    {"property": "Статус", "select": {"does_not_equal": "✅ Сдано"}},
                    {"property": "Статус", "select": {"does_not_equal": "❌ Отменено"}},
                ]
            },
        )
        return res.get("results", [])
    except Exception as e:
        logger.error(f"get_overdue: {e}")
        return []


def get_due_soon(days: int = 1) -> list[dict]:
    try:
        today = date.today()
        cutoff = (today + timedelta(days=days)).isoformat()
        res = notion.databases.query(
            database_id=TASKS_DB,
            filter={
                "and": [
                    {"property": "Дедлайн", "date": {"on_or_before": cutoff}},
                    {"property": "Дедлайн", "date": {"on_or_after": today.isoformat()}},
                    {"property": "Статус", "select": {"does_not_equal": "✅ Сдано"}},
                    {"property": "Статус", "select": {"does_not_equal": "❌ Отменено"}},
                ]
            },
        )
        return res.get("results", [])
    except Exception as e:
        logger.error(f"get_due_soon: {e}")
        return []


def get_waiting_stale() -> list[dict]:
    try:
        cutoff = (date.today() - timedelta(days=WAITING_ALERT_DAYS)).isoformat()
        res = notion.databases.query(
            database_id=TASKS_DB,
            filter={
                "and": [
                    {"property": "Ожидание с", "date": {"on_or_before": cutoff}},
                    {"property": "Ожидаем отдел", "select": {"does_not_equal": "—"}},
                ]
            },
        )
        return res.get("results", [])
    except Exception as e:
        logger.error(f"get_waiting_stale: {e}")
        return []


def get_patterns_context() -> str:
    try:
        res = notion.databases.query(database_id=PATTERNS_DB, page_size=8)
        lines = []
        for p in res.get("results", []):
            pr = p["properties"]
            name = _get_title(pr, "Паттерн")
            wf   = _get_rich(pr, "Типичный воркфлоу")
            iters = pr.get("Среднее кол-во итераций", {}).get("number", "?")
            notes = _get_rich(pr, "Заметки")
            lines.append(f"- {name}: {wf} | avg_iters={iters} | {notes}")
        return "\n".join(lines)
    except Exception as e:
        logger.error(f"get_patterns_context: {e}")
        return ""


# ─────────────────────────────────────────────────────────────────────────────
# Дневник активности
# ─────────────────────────────────────────────────────────────────────────────
def log_event(
    event: str,
    project: str,
    event_type: str,
    episode: str = "",
    assignee: str = "",
    tg_link: str = "",
) -> None:
    """Записывает событие в базу Дневник активности в Notion."""
    try:
        props: dict = {
            "Событие":    {"title": _rich_text(event[:200])},
            "Дата":       {"date": {"start": date.today().isoformat()}},
            "Проект":     {"rich_text": _rich_text(project)},
            "Тип":        {"select": {"name": event_type}},
        }
        if episode:
            props["Выпуск"] = {"rich_text": _rich_text(episode)}
        if assignee:
            props["Исполнитель"] = {"rich_text": _rich_text(assignee)}
        if tg_link:
            props["Ссылка TG"] = {"url": tg_link}
        notion.pages.create(parent={"database_id": DIARY_DB}, properties=props)
    except Exception as e:
        logger.error(f"log_event: {e}")


def create_project(topic_name: str, thread_id: int, chat_id: int) -> bool:
    """Создаёт новый проект в Notion при появлении нового топика в Telegram."""
    try:
        # Проверяем — вдруг уже есть
        res = notion.databases.query(
            database_id=PROJECTS_DB,
            filter={"property": "Топик ID", "number": {"equals": thread_id}},
            page_size=1,
        )
        if res.get("results"):
            return False  # уже существует
        notion.pages.create(
            parent={"database_id": PROJECTS_DB},
            properties={
                "Проект":         {"title": _rich_text(topic_name)},
                "Telegram топик": {"rich_text": _rich_text(topic_name)},
                "Топик ID":       {"number": thread_id},
                "Статус проекта": {"select": {"name": "🟢 Активен"}},
            },
        )
        log_event(
            event=f"Новый топик: {topic_name}",
            project=topic_name,
            event_type="👥 Топик создан",
        )
        return True
    except Exception as e:
        logger.error(f"create_project: {e}")
        return False


# ─────────────────────────────────────────────────────────────────────────────
# Ежедневный отчёт
# ─────────────────────────────────────────────────────────────────────────────
async def generate_daily_report(target_date: date) -> None:
    """Генерирует GPT-сводку за указанный день и сохраняет в Notion."""
    try:
        # Берём события за указанный день из Дневника
        res = notion.databases.query(
            database_id=DIARY_DB,
            filter={"property": "Дата", "date": {"equals": target_date.isoformat()}},
            page_size=100,
        )
        events = res.get("results", [])
        if not events:
            logger.info(f"daily_report: no events for {target_date}")
            return

        # Группируем по проекту
        by_project: dict[str, list[str]] = {}
        for e in events:
            p = e["properties"]
            project = p.get("Проект", {}).get("rich_text", [{}])
            project = project[0].get("text", {}).get("content", "—") if project else "—"
            event_type = (p.get("Тип", {}).get("select") or {}).get("name", "")
            event_name = (p.get("Событие", {}).get("title") or [{}])
            event_name = event_name[0].get("text", {}).get("content", "") if event_name else ""
            episode = (p.get("Выпуск", {}).get("rich_text") or [{}])
            episode = episode[0].get("text", {}).get("content", "") if episode else ""
            line = f"- [{event_type}] {event_name}" + (f" (#{episode})" if episode else "")
            by_project.setdefault(project, []).append(line)

        # Формируем промпт для GPT
        events_text = ""
        for proj, lines in by_project.items():
            events_text += f"\nПроект: {proj}\n" + "\n".join(lines) + "\n"

        prompt = (
            f"Ты ассистент пост-продакшн студии. Составь дневной отчёт за {target_date.strftime('%d.%m.%Y')}.\n\n"
            f"Ниже — всё что происходило в каждой рабочей ветке за день. "
            f"Включает переписку, обсуждения, правки, смены статусов и создание задач.\n\n"
            f"{events_text}\n\n"
            f"Для каждого проекта напиши 3-5 предложений: что обсуждалось, какая работа велась, "
            f"какие решения приняты, на каком этапе сейчас. "
            f"Пиши как будто объясняешь руководителю что происходило в команде за день. "
            f"Стиль — деловой, конкретный, без воды. "
            f"Формат: ### Название проекта, затем текст."
        )

        resp = await openai_client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[{"role": "user", "content": prompt}],
            temperature=0.3,
            max_tokens=1200,
        )
        summary = resp.choices[0].message.content.strip()

        # Статистика
        n_tasks   = sum(1 for e in events if "Задача создана" in str(e["properties"].get("Тип", "")))
        n_status  = sum(1 for e in events if "Статус" in str(e["properties"].get("Тип", "")))
        n_projects = len(by_project)

        # Создаём страницу в Ежедневные отчёты
        # Режем summary на куски по 2000 символов (лимит Notion на rich_text блок)
        def _summary_blocks(text: str) -> list:
            chunks = [text[i:i+2000] for i in range(0, len(text), 2000)]
            return [
                {"object": "block", "type": "paragraph",
                 "paragraph": {"rich_text": [{"type": "text", "text": {"content": c}}]}}
                for c in chunks
            ]

        page = notion.pages.create(
            parent={"database_id": REPORTS_DB},
            properties={
                "Дата":               {"title": _rich_text(target_date.strftime("%d.%m.%Y"))},
                "Проектов активных":  {"number": n_projects},
                "Задач создано":      {"number": n_tasks},
                "Статусов изменено":  {"number": n_status},
                "Итого событий":      {"number": len(events)},
            },
            children=_summary_blocks(summary) if summary else [],
        )
        logger.info(f"daily_report created for {target_date}: {page['id']}")
        return summary  # возвращаем текст для отправки в Telegram

    except Exception as e:
        logger.error(f"generate_daily_report: {e}")
        return None


async def job_daily_report(context: ContextTypes.DEFAULT_TYPE) -> None:
    """Scheduled job: генерирует отчёт за вчера и отправляет в Telegram в 8:50."""
    yesterday = date.today() - timedelta(days=1)
    summary = await generate_daily_report(yesterday)

    chats = _all_digest_chats(context.bot_data)
    if not summary or not chats:
        return

    header = f"📋 *Отчёт за {yesterday.strftime('%d.%m.%Y')}*\n\n"
    full_text = header + summary

    # Telegram ограничивает сообщение 4096 символами — режем если нужно
    chunks = [full_text[i:i+4000] for i in range(0, len(full_text), 4000)]
    for chat_id in chats:
        for chunk in chunks:
            try:
                await context.bot.send_message(
                    chat_id=chat_id,
                    text=chunk,
                    disable_web_page_preview=True,
                )
            except Exception as e:
                logger.error(f"daily_report send error {chat_id}: {e}")


# ─────────────────────────────────────────────────────────────────────────────
# GPT helpers
# ─────────────────────────────────────────────────────────────────────────────
_GPT_SYSTEM = """Ты ассистент по управлению задачами в компании видеомонтажа (пост-продакшен).
Анализируй сообщение из Telegram и определяй: это новая задача, обновление существующей или просто разговор.

Паттерны проектов компании:
{patterns}

Верни ТОЛЬКО валидный JSON (без markdown) с полями:
- is_task (bool): это задача (новая или апдейт)?
- is_new_task (bool): это НОВАЯ задача (не продолжение существующей)?
- task_name (string): краткое название задачи, макс 100 символов, на русском
- status (string): один из статусов [🆕 Новая, ✂️ Монтаж, 🔄 Правки, 👁️ Финальный просмотр, 📤 Отправлено клиенту, 🟡 На согласовании, 🎨 Ждём цвет, 🔊 Ждём звук, ✨ Ждём графику, ✅ Сдано, ⏸️ Заморожено]
- assignees (list[str]): @username без знака @
- deadline_iso (str|null): дата в формате YYYY-MM-DD если упоминается
- summary (string): 1–2 предложения о контексте на русском
- confidence (float 0–1): уверенность что это задача

Будь консервативен: если сомневаешься — is_task=false, confidence<0.5.
"""


async def gpt_analyze(text: str, topic: str, episode: str, patterns: str) -> dict:
    try:
        system = _GPT_SYSTEM.format(patterns=patterns or "нет данных")
        today_str = date.today().strftime("%d.%m.%Y")
        user = f"Сегодня: {today_str}\nВетка: {topic}\nВыпуск: {episode or 'не указан'}\nСообщение: {text}"
        resp = await openai_client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[{"role": "system", "content": system}, {"role": "user", "content": user}],
            temperature=0.15,
            max_tokens=400,
        )
        content = resp.choices[0].message.content.strip()
        return json.loads(content)
    except json.JSONDecodeError:
        logger.warning("GPT returned non-JSON")
        return {"is_task": False, "confidence": 0}
    except Exception as e:
        logger.error(f"gpt_analyze: {e}")
        return {"is_task": False, "confidence": 0}


async def gpt_transcribe(audio_bytes: bytes) -> str:
    try:
        buf = io.BytesIO(audio_bytes)
        buf.name = "voice.ogg"
        transcript = await openai_client.audio.transcriptions.create(
            model="whisper-1", file=buf, language="ru"
        )
        return transcript.text
    except Exception as e:
        logger.error(f"gpt_transcribe: {e}")
        return ""


# ─────────────────────────────────────────────────────────────────────────────
# Форматирование для дайджеста
# ─────────────────────────────────────────────────────────────────────────────
def fmt_task_line(task: dict) -> str:
    p = task["properties"]
    name     = _get_title(p)[:55] or "Без названия"
    project  = _get_select(p, "Проект")
    episode  = _get_select(p, "Выпуск")
    assignee = _get_rich(p, "Исполнитель")
    tg_url   = p.get("Ссылка TG", {}).get("url", "")
    ep_str   = f" · #{escape_md(episode)}" if episode and episode != "—" else ""
    link     = f" [→]({tg_url})" if tg_url else ""
    return f"  • {escape_md(name)} | {escape_md(project)}{ep_str} | {escape_md(assignee)}{link}"


def fmt_deadline_line(task: dict) -> str:
    p = task["properties"]
    name     = _get_title(p)[:50] or "Без названия"
    dl       = p.get("Дедлайн", {}).get("date") or {}
    deadline = dl.get("start", "?")
    assignee = _get_rich(p, "Исполнитель")
    project  = _get_select(p, "Проект")
    return f"  • {escape_md(name)} | {escape_md(project)} | {escape_md(assignee)} | 📅 {deadline}"


# ─────────────────────────────────────────────────────────────────────────────
# Карточка задачи с inline-кнопками статусов
# ─────────────────────────────────────────────────────────────────────────────
async def _show_task_card(
    msg,
    task: dict,
    bot_data: dict,
    thread_id: Optional[int] = None,
) -> None:
    """
    Отправляет карточку задачи с кнопками быстрой смены статуса.
    Callback-данные хранятся в bot_data под ключом sc_<key>.
    """
    p        = task["properties"]
    name     = _get_title(p)[:80] or "Без названия"
    project  = _get_select(p, "Проект")
    episode  = _get_select(p, "Выпуск")
    status   = _get_select(p, "Статус")
    assignee = _get_rich(p, "Исполнитель")
    dl       = (p.get("Дедлайн") or {}).get("date") or {}
    deadline = dl.get("start", "—") if isinstance(dl, dict) else "—"
    iters    = (p.get("Итерация правок") or {}).get("number", 0)
    tg_url   = p.get("Ссылка TG", {}).get("url", "")

    ep_str = f" · #{escape_md(episode)}" if episode and episode != "—" else ""
    iter_str = f"  · 🔄 ×{iters}" if iters else ""

    text = (
        f"📋 *{escape_md(name)}*\n"
        f"📁 {escape_md(project)}{ep_str}\n"
        f"📊 *Статус:* {escape_md(status)}\n"
        f"👤 {escape_md(assignee)}\n"
        f"📅 {escape_md(deadline)}{iter_str}"
        + (f"\n[→ Открыть сообщение]({tg_url})" if tg_url else "")
    )

    # Сохраняем page_id под коротким ключом (64-байтовое ограничение callback_data)
    task_key = _short_key()
    bot_data[f"sc_{task_key}"] = task["id"]

    def _cb(action: str) -> str:
        return f"sc:{task_key}:{action}"

    keyboard = InlineKeyboardMarkup([
        [
            InlineKeyboardButton("✂️ Монтаж",    callback_data=_cb("inprogress")),
            InlineKeyboardButton("🔄 Правки",    callback_data=_cb("review")),
            InlineKeyboardButton("🟡 Согласов.", callback_data=_cb("approval")),
        ],
        [
            InlineKeyboardButton("🎨 Цвет",      callback_data=_cb("color")),
            InlineKeyboardButton("🔊 Звук",      callback_data=_cb("sound")),
            InlineKeyboardButton("✅ Сдано",     callback_data=_cb("done")),
        ],
        [
            InlineKeyboardButton("👁️ Финал",     callback_data=_cb("final")),
            InlineKeyboardButton("📤 Клиенту",   callback_data=_cb("sent")),
            InlineKeyboardButton("⏸️ Стоп",      callback_data=_cb("freeze")),
        ],
    ])

    await msg.reply_text(
        text,
        parse_mode="Markdown",
        reply_markup=keyboard,
        disable_web_page_preview=True,
        message_thread_id=thread_id,
    )


# ─────────────────────────────────────────────────────────────────────────────
# Обработчик сообщений
# ─────────────────────────────────────────────────────────────────────────────
async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    message = update.message
    if not message:
        return

    thread_id = message.message_thread_id

    # Определяем название топика через Notion (по числовому ID)
    topic = None  # None = General, не обрабатываем
    if message.is_topic_message and thread_id:
        fc = getattr(message, "forum_topic_created", None)
        if fc:
            # Это сообщение-создание топика — регистрируем сразу
            topic = fc.name
        else:
            notion_name = get_project_name(thread_id)
            if notion_name:
                topic = notion_name
            else:
                # Топик не зарегистрирован — пробуем авторегистрацию
                # В Telegram reply_to_message первого сообщения в топике
                # содержит forum_topic_created с именем топика
                auto_name: Optional[str] = None
                rpl = getattr(message, "reply_to_message", None)
                if rpl:
                    fc2 = getattr(rpl, "forum_topic_created", None)
                    if fc2:
                        auto_name = fc2.name

                if auto_name:
                    created = create_project(auto_name, thread_id, message.chat_id)
                    if created:
                        logger.info(f"Auto-registered topic '{auto_name}' id={thread_id}")
                        await message.reply_text(
                            f"📁 Топик *{escape_md(auto_name)}* авторегистрирован в Notion!",
                            parse_mode="Markdown",
                            message_thread_id=thread_id,
                        )
                    topic = auto_name
                else:
                    # Имя не удалось определить — подсказываем один раз
                    reg_key = f"reg_hint_{thread_id}"
                    if not context.bot_data.get(reg_key):
                        context.bot_data[reg_key] = True
                        await message.reply_text(
                            "⚠️ Этот топик не зарегистрирован в базе.\n"
                            "Введи: `/setup Название проекта`\n"
                            "_(например: /setup СОЛНЫШКО)_",
                            parse_mode="Markdown",
                            message_thread_id=thread_id,
                        )
                    topic = f"Топик-{thread_id}"  # временное имя

    # Сообщения из General (не топик) — игнорируем, только команды
    if not topic:
        return

    # Голосовое → транскрипция
    text = message.text or message.caption or ""
    if message.voice:
        await message.reply_text("🎙️ Транскрибирую...", message_thread_id=thread_id)
        voice_file = await message.voice.get_file()
        raw = await voice_file.download_as_bytearray()
        text = await gpt_transcribe(bytes(raw))
        if not text:
            await message.reply_text("❌ Не удалось распознать голосовое", message_thread_id=thread_id)
            return
        await message.reply_text(f"📝 _{escape_md(text)}_", parse_mode="Markdown", message_thread_id=thread_id)

    if not text:
        return

    hashtags  = parse_hashtags(text)
    mentions  = parse_mentions(text)
    all_links = extract_urls(text)
    file_links = [l for l in all_links if is_file_link(l)]
    episode   = hashtags[0] if hashtags else ""

    # ── Проверка обязательности #хэштега ──────────────────────────────────
    if project_uses_hashtags(topic) and not hashtags:
        author = f"@{message.from_user.username}" if (message.from_user and message.from_user.username) else "автор"
        await message.reply_text(
            f"⚠️ {author}, укажи *#выпуск* в сообщении — в этой ветке теги обязательны. Задача не создана.",
            parse_mode="Markdown",
            message_thread_id=thread_id,
        )
        return

    tg_link = build_tg_link(message.chat_id, message.message_id)

    # ── Только ссылки без явного описания задачи → прикрепляем к активной ──
    task_keywords = ("задача", "сделать", "монтаж", "правки", "нужно", "готово", "сдал", "отправил", "проверь")
    only_links = file_links and not any(kw in text.lower() for kw in task_keywords)
    if only_links:
        active = find_active_task(topic, episode)
        if active:
            append_links(active["id"], file_links)
            await message.reply_text(
                f"🔗 Ссылк{'а прикреплена' if len(file_links) == 1 else 'и прикреплены'} к задаче{' #' + episode if episode else ''}",
                parse_mode="Markdown",
                message_thread_id=thread_id,
            )
            return

    # ── GPT-анализ ──────────────────────────────────────────────────────────
    patterns = get_patterns_context()
    analysis = await gpt_analyze(text, topic, episode, patterns)

    if not analysis.get("is_task") or analysis.get("confidence", 0) < GPT_CONFIDENCE_THRESHOLD:
        # Не задача — тихо прикрепляем ссылки если есть
        if file_links:
            active = find_active_task(topic, episode)
            if active:
                append_links(active["id"], file_links)
        # Логируем активность в дневник даже если не задача (если GPT дал summary)
        gpt_summary = analysis.get("summary", "")
        if gpt_summary and analysis.get("confidence", 0) > 0.2:
            log_event(
                event=gpt_summary[:200],
                project=topic,
                event_type="💬 Активность",
                episode=episode,
                assignee=", ".join(f"@{a}" for a in (analysis.get("assignees") or mentions)),
                tg_link=build_tg_link(message.chat_id, message.message_id),
            )
        return

    task_name   = analysis.get("task_name") or text[:100]
    status      = analysis.get("status", "🆕 Новая")
    assignees   = analysis.get("assignees") or mentions
    deadline_iso = analysis.get("deadline_iso")
    summary     = analysis.get("summary", "")

    # Корректируем год дедлайна если GPT вернул прошедший год
    if deadline_iso:
        try:
            dl = date.fromisoformat(deadline_iso)
            today = date.today()
            if dl < today:
                dl = dl.replace(year=today.year)
                if dl < today:
                    dl = dl.replace(year=today.year + 1)
                deadline_iso = dl.isoformat()
        except ValueError:
            deadline_iso = None

    # ── Обновление существующей задачи ────────────────────────────────────
    if not analysis.get("is_new_task", True):
        active = find_active_task(topic, episode)
        if active:
            update_status(active["id"], status, increment_iter=(status == "🔄 Правки"))
            if file_links:
                append_links(active["id"], file_links)
            await message.reply_text(
                f"📝 Задача обновлена: *{status}*",
                parse_mode="Markdown",
                message_thread_id=thread_id,
            )
            return

    # ── Показываем кнопки подтверждения ────────────────────────────────────
    pending_key = f"p_{message.chat_id}_{message.message_id}"
    context.bot_data[pending_key] = {
        "task_name":  task_name,
        "project":    topic,
        "episode":    episode,
        "assignees":  assignees,
        "tg_link":    tg_link,
        "topic":      topic,
        "status":     status,
        "deadline":   deadline_iso,
        "links":      file_links,
        "summary":    summary,
        "thread_id":  thread_id,
        "chat_id":    message.chat_id,
    }

    ep_str     = f"#{episode}" if episode else "—"
    assign_str = " ".join(f"@{a}" for a in assignees) if assignees else "—"
    dl_str     = deadline_iso or "—"

    keyboard = InlineKeyboardMarkup([[
        InlineKeyboardButton("✅ Создать", callback_data=f"confirm:{pending_key}"),
        InlineKeyboardButton("❌ Отмена",  callback_data=f"cancel:{pending_key}"),
    ]])

    await message.reply_text(
        f"🤖 *Создать задачу?*\n\n"
        f"📝 {escape_md(task_name)}\n"
        f"📁 *Проект:* {escape_md(topic)}\n"
        f"🔖 *Выпуск:* {escape_md(ep_str)}\n"
        f"👤 *Исполнитель:* {escape_md(assign_str)}\n"
        f"📊 *Статус:* {escape_md(status)}\n"
        f"📅 *Дедлайн:* {escape_md(dl_str)}",
        parse_mode="Markdown",
        reply_markup=keyboard,
        message_thread_id=thread_id,
    )


# ─────────────────────────────────────────────────────────────────────────────
# Обработчик кнопок подтверждения
# ─────────────────────────────────────────────────────────────────────────────
async def handle_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()
    data = query.data or ""

    if data.startswith("confirm:"):
        key = data[8:]
        td  = context.bot_data.pop(key, None)
        if not td:
            await query.edit_message_text("⏰ Время подтверждения истекло — отправь сообщение снова")
            return
        page_id = create_task(
            task_name = td["task_name"],
            project   = td["project"],
            episode   = td["episode"],
            assignees = td["assignees"],
            tg_link   = td["tg_link"],
            topic     = td["topic"],
            status    = td["status"],
            deadline  = td.get("deadline"),
            links     = td.get("links"),
            ai_summary= td.get("summary", ""),
            chat_id   = td.get("chat_id"),
            thread_id = td.get("thread_id"),
        )
        if page_id:
            ep = f" · #{td['episode']}" if td["episode"] else ""
            await query.edit_message_text(
                f"✅ *Задача создана{escape_md(ep)}*\n_{escape_md(td['task_name'])}_",
                parse_mode="Markdown",
            )
            # Логируем в дневник
            log_event(
                event=td["task_name"],
                project=td["project"],
                event_type="🆕 Задача создана",
                episode=td.get("episode", ""),
                assignee=", ".join(f"@{a}" for a in td.get("assignees", [])),
                tg_link=td.get("tg_link", ""),
            )
        else:
            await query.edit_message_text("❌ Ошибка при создании задачи в Notion")

    elif data.startswith("cancel:"):
        context.bot_data.pop(data[7:], None)
        await query.edit_message_text("❌ Отменено")

    elif data.startswith("sc:"):
        # sc:<task_key>:<action>  — кнопки статуса из карточки задачи
        _SC_MAP: dict[str, tuple[str, bool]] = {
            "inprogress": ("✂️ Монтаж",              False),
            "review":     ("🔄 Правки",              True),
            "final":      ("👁️ Финальный просмотр", False),
            "sent":       ("📤 Отправлено клиенту", False),
            "approval":   ("🟡 На согласовании",    False),
            "done":       ("✅ Сдано",              False),
            "freeze":     ("⏸️ Заморожено",          False),
            "color":      ("🎨 Ждём цвет",          False),
            "sound":      ("🔊 Ждём звук",          False),
        }
        parts = data.split(":", 2)
        if len(parts) < 3:
            return
        task_key, action = parts[1], parts[2]
        page_id = context.bot_data.pop(f"sc_{task_key}", None)
        if not page_id:
            await query.edit_message_text("⏰ Устарело — выполни команду снова")
            return
        if action not in _SC_MAP:
            await query.edit_message_text("❓ Неизвестное действие")
            return
        status, increment_iter = _SC_MAP[action]
        ok = update_status(page_id, status, increment_iter=increment_iter)
        suffix = " (+1 итерация)" if increment_iter else ""
        if ok:
            await query.edit_message_text(
                f"✅ Статус → *{escape_md(status)}*{suffix}",
                parse_mode="Markdown",
            )
            log_event(
                event=f"Статус изменён → {status} (кнопка карточки)",
                project="—",
                event_type="📝 Статус изменён" if status != "🔄 Правки" else "🔄 Правки",
            )
        else:
            await query.edit_message_text("❌ Ошибка при обновлении статуса")


# ─────────────────────────────────────────────────────────────────────────────
# Команды — статусы (reply на сообщение задачи)
# ─────────────────────────────────────────────────────────────────────────────
async def _set_status(
    update: Update, context: ContextTypes.DEFAULT_TYPE,
    status: str, emoji: str, increment_iter: bool = False,
) -> None:
    msg = update.message
    tid = msg.message_thread_id
    if not msg.reply_to_message:
        await msg.reply_text("↩️ Ответьте этой командой на *сообщение с задачей*",
                             parse_mode="Markdown", message_thread_id=tid)
        return
    tg_link = build_tg_link(msg.chat_id, msg.reply_to_message.message_id)
    page_id = find_task_by_tg_link(tg_link)
    if page_id:
        update_status(page_id, status, increment_iter=increment_iter)
        suffix = " (+1 итерация)" if increment_iter else ""
        await msg.reply_text(f"{emoji} Статус → *{status}*{suffix}",
                             parse_mode="Markdown", message_thread_id=tid)
        # Логируем смену статуса в Дневник
        project = get_project_name(tid) if tid else "—"
        log_event(
            event=f"Статус изменён → {status}",
            project=project or "—",
            event_type="📝 Статус изменён" if status != "🔄 Правки" else "🔄 Правки",
            tg_link=tg_link,
        )
    else:
        await msg.reply_text("❓ Задача не найдена в базе", message_thread_id=tid)


async def cmd_done(u, c):       await _set_status(u, c, "✅ Сдано",                "✅")
async def cmd_inprogress(u, c): await _set_status(u, c, "✂️ Монтаж",              "✂️")
async def cmd_review(u, c):     await _set_status(u, c, "🔄 Правки",              "🔄", increment_iter=True)
async def cmd_final(u, c):      await _set_status(u, c, "👁️ Финальный просмотр", "👁️")
async def cmd_freeze(u, c):     await _set_status(u, c, "⏸️ Заморожено",          "⏸️")

# Короткие алиасы (те же обработчики)
cmd_d  = cmd_done        # /d  → /done
cmd_ip = cmd_inprogress  # /ip → /inprogress
cmd_r  = cmd_review      # /r  → /review
cmd_f  = cmd_final       # /f  → /final


# ─────────────────────────────────────────────────────────────────────────────
# Команды — управление
# ─────────────────────────────────────────────────────────────────────────────
async def cmd_deadline(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    msg = update.message
    tid = msg.message_thread_id
    if not msg.reply_to_message:
        await msg.reply_text("↩️ Ответьте командой на задачу\n_Пример: /deadline 25 марта_",
                             parse_mode="Markdown", message_thread_id=tid)
        return
    if not context.args:
        await msg.reply_text("📅 Укажи дату: `/deadline 25 марта` или `/deadline 25.03`",
                             parse_mode="Markdown", message_thread_id=tid)
        return
    dl = parse_date_input(" ".join(context.args))
    if not dl:
        await msg.reply_text(f"❓ Не понял дату. Попробуй: `25 марта`, `25.03`, `завтра`",
                             parse_mode="Markdown", message_thread_id=tid)
        return
    tg_link = build_tg_link(msg.chat_id, msg.reply_to_message.message_id)
    page_id = find_task_by_tg_link(tg_link)
    if page_id:
        update_deadline(page_id, dl)
        await msg.reply_text(f"📅 Дедлайн: *{dl}*", parse_mode="Markdown", message_thread_id=tid)
    else:
        await msg.reply_text("❓ Задача не найдена в базе", message_thread_id=tid)


async def cmd_assign(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    msg = update.message
    tid = msg.message_thread_id
    if not msg.reply_to_message or not context.args:
        await msg.reply_text("↩️ Ответьте командой на задачу и укажи @username\n_Пример: /assign @kate_",
                             parse_mode="Markdown", message_thread_id=tid)
        return
    assignee = context.args[0].lstrip("@")
    tg_link  = build_tg_link(msg.chat_id, msg.reply_to_message.message_id)
    page_id  = find_task_by_tg_link(tg_link)
    if page_id:
        update_assignee(page_id, assignee)
        await msg.reply_text(f"👤 Назначен: *@{assignee}*", parse_mode="Markdown", message_thread_id=tid)
    else:
        await msg.reply_text("❓ Задача не найдена в базе", message_thread_id=tid)


async def cmd_waiting(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    msg = update.message
    tid = msg.message_thread_id
    if not msg.reply_to_message:
        await msg.reply_text("↩️ Ответьте командой на задачу\n_Пример: /waiting звук_",
                             parse_mode="Markdown", message_thread_id=tid)
        return
    dept = (context.args[0].lower() if context.args else "")
    if dept not in ("цвет", "звук", "графику", "графика"):
        await msg.reply_text("🏢 Укажи отдел: `/waiting цвет` · `/waiting звук` · `/waiting графику`",
                             parse_mode="Markdown", message_thread_id=tid)
        return
    tg_link = build_tg_link(msg.chat_id, msg.reply_to_message.message_id)
    page_id = find_task_by_tg_link(tg_link)
    if page_id:
        set_waiting(page_id, dept)
        emoji = {"цвет": "🎨", "звук": "🔊", "графику": "✨", "графика": "✨"}[dept]
        await msg.reply_text(
            f"{emoji} Ждём *{dept}*. Напомню если нет ответа через {WAITING_ALERT_DAYS} дня.",
            parse_mode="Markdown", message_thread_id=tid,
        )
    else:
        await msg.reply_text("❓ Задача не найдена в базе", message_thread_id=tid)


# ─────────────────────────────────────────────────────────────────────────────
# Команда /report — дайджест
# ─────────────────────────────────────────────────────────────────────────────
async def cmd_report(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    msg = update.message
    tid = msg.message_thread_id
    await msg.reply_text("⏳ Собираю...", message_thread_id=tid)

    active_sections = [
        ("🆕 Новые",             "🆕 Новая"),
        ("✂️ В монтаже",         "✂️ Монтаж"),
        ("🔄 На правках",        "🔄 Правки"),
        ("👁️ Финальный просмотр","👁️ Финальный просмотр"),
        ("🎨 Ждём цвет",         "🎨 Ждём цвет"),
        ("🔊 Ждём звук",         "🔊 Ждём звук"),
        ("✨ Ждём графику",       "✨ Ждём графику"),
    ]

    lines = [f"📊 *Дайджест — {datetime.now().strftime('%d.%m.%Y')}*\n"]
    total = 0
    for label, status in active_sections:
        tasks = get_tasks_by_status(status)
        if tasks:
            lines.append(f"\n{label} — {len(tasks)}")
            for t in tasks[:7]:
                lines.append(fmt_task_line(t))
            total += len(tasks)

    overdue = get_overdue()
    if overdue:
        lines.append(f"\n🔥 *Просрочено* — {len(overdue)}")
        for t in overdue[:5]:
            lines.append(fmt_deadline_line(t))

    lines.append(
        f"\n_Всего активных: {total}_" if total else "\n🎉 Все задачи завершены!"
    )

    await msg.reply_text(
        "\n".join(lines),
        parse_mode="Markdown",
        disable_web_page_preview=True,
        message_thread_id=tid,
    )


async def cmd_topicid(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Отвечает числовым ID текущего топика — для заполнения Notion."""
    msg = update.message
    tid = msg.message_thread_id
    chat_id = msg.chat_id
    if tid:
        await msg.reply_text(
            f"🔢 *Топик ID:* `{tid}`\n"
            f"Chat ID: `{chat_id}`\n\n"
            f"Вставь `{tid}` в колонку *Топик ID* этого проекта в Notion.",
            parse_mode="Markdown",
            message_thread_id=tid,
        )
    else:
        await msg.reply_text(
            f"ℹ️ Это не топик-тред.\nChat ID: `{chat_id}`",
            parse_mode="Markdown",
        )


# ─────────────────────────────────────────────────────────────────────────────
# Новый UX: команды по имени проекта + номеру выпуска
# ─────────────────────────────────────────────────────────────────────────────

# Алиасы для русских ключевых слов → точный статус Notion
STATUS_ALIASES: dict[str, tuple[str, bool]] = {
    # keyword:  (статус Notion, increment_iter)
    "монтаж":      ("✂️ Монтаж",              False),
    "правки":      ("🔄 Правки",              True),
    "правка":      ("🔄 Правки",              True),
    "финал":       ("👁️ Финальный просмотр", False),
    "просмотр":    ("👁️ Финальный просмотр", False),
    "клиент":      ("📤 Отправлено клиенту", False),
    "отправлено":  ("📤 Отправлено клиенту", False),
    "согласование":("🟡 На согласовании",    False),
    "согласов":    ("🟡 На согласовании",    False),
    "сдано":       ("✅ Сдано",              False),
    "готово":      ("✅ Сдано",              False),
    "done":        ("✅ Сдано",              False),
    "заморожено":  ("⏸️ Заморожено",          False),
    "стоп":        ("⏸️ Заморожено",          False),
    "freeze":      ("⏸️ Заморожено",          False),
    "цвет":        ("🎨 Ждём цвет",          False),
    "звук":        ("🔊 Ждём звук",          False),
    "графика":     ("✨ Ждём графику",       False),
    "графику":     ("✨ Ждём графику",       False),
}


async def cmd_task(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    /task <проект> <выпуск>
    Показывает карточку активной задачи с кнопками статуса.
    Пример: /task авито 25
    """
    msg = update.message
    tid = msg.message_thread_id

    if not context.args:
        await msg.reply_text(
            "📋 Использование: `/task <проект> <выпуск>`\n"
            "_Пример: /task авито 25_",
            parse_mode="Markdown",
            message_thread_id=tid,
        )
        return

    # Поддержка многословных проектов: /task не мои мысли 2
    # Последний аргумент = выпуск, всё предыдущее = название проекта
    # Подчёркивание работает как пробел: /task не_мои_мысли 2
    if len(context.args) >= 2:
        project_query = " ".join(context.args[:-1]).replace("_", " ")
        episode_query = context.args[-1]
    else:
        project_query = context.args[0].replace("_", " ")
        episode_query = ""

    project_name = find_project_fuzzy(project_query)
    if not project_name:
        await msg.reply_text(
            f"❓ Проект *{escape_md(project_query)}* не найден.\n"
            f"Проверь название или создай топик в группе.",
            parse_mode="Markdown",
            message_thread_id=tid,
        )
        return

    task = find_active_task(project_name, episode_query)
    if not task:
        ep_str = f" · #{escape_md(episode_query)}" if episode_query else ""
        await msg.reply_text(
            f"📭 Активных задач не найдено: *{escape_md(project_name)}{ep_str}*\n\n"
            f"Напиши задачу в ветке проекта, и бот предложит её создать.",
            parse_mode="Markdown",
            message_thread_id=tid,
        )
        return

    await _show_task_card(msg, task, context.bot_data, thread_id=tid)


async def cmd_status_ref(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    /s <проект> <выпуск> <статус>
    Меняет статус задачи без reply, прямо по имени проекта и выпуска.
    Пример: /s авито 25 монтаж
    Пример: /s авито 25 сдано
    """
    msg = update.message
    tid = msg.message_thread_id

    if len(context.args) < 3:
        await msg.reply_text(
            "📊 Использование: `/s <проект> <выпуск> <статус>`\n\n"
            "*Доступные статусы:*\n"
            "`монтаж` · `правки` · `финал` · `клиент`\n"
            "`согласование` · `сдано` · `стоп`\n"
            "`цвет` · `звук` · `графика`\n\n"
            "_Пример: /s авито 25 монтаж_\n"
            "_Пример: /s не_мои_мысли 2 правки_",
            parse_mode="Markdown",
            message_thread_id=tid,
        )
        return

    # Поддержка многословных проектов: /s не мои мысли 2 монтаж
    # Последний аргумент = статус, предпоследний = выпуск, остальные = проект
    # Подчёркивание работает как пробел: /s не_мои_мысли 2 монтаж
    status_key    = context.args[-1].lower()
    episode_query = context.args[-2]
    project_query = " ".join(context.args[:-2]).replace("_", " ") if len(context.args) > 3 else context.args[0].replace("_", " ")

    if status_key not in STATUS_ALIASES:
        await msg.reply_text(
            f"❓ Статус *{escape_md(status_key)}* не распознан.\n"
            f"Используй: `монтаж`, `правки`, `финал`, `клиент`, `согласование`, `сдано`, `стоп`",
            parse_mode="Markdown",
            message_thread_id=tid,
        )
        return

    project_name = find_project_fuzzy(project_query)
    if not project_name:
        await msg.reply_text(
            f"❓ Проект *{escape_md(project_query)}* не найден.",
            parse_mode="Markdown",
            message_thread_id=tid,
        )
        return

    task = find_active_task(project_name, episode_query)
    if not task:
        ep_str = f" · #{escape_md(episode_query)}" if episode_query else ""
        await msg.reply_text(
            f"📭 Активных задач не найдено: *{escape_md(project_name)}{ep_str}*",
            parse_mode="Markdown",
            message_thread_id=tid,
        )
        return

    status, increment_iter = STATUS_ALIASES[status_key]
    update_status(task["id"], status, increment_iter=increment_iter)
    suffix = " (+1 итерация)" if increment_iter else ""
    await msg.reply_text(
        f"📊 *{escape_md(project_name)}* · #{escape_md(episode_query)}\n"
        f"Статус → *{escape_md(status)}*{suffix}",
        parse_mode="Markdown",
        message_thread_id=tid,
    )
    log_event(
        event=f"Статус изменён → {status} (команда /s)",
        project=project_name,
        event_type="📝 Статус изменён" if status != "🔄 Правки" else "🔄 Правки",
        episode=episode_query,
    )


async def cmd_smart_project(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    Ловит команды вида /авито 25 или /авито_25:
    ищет проект по имени и показывает карточку задачи.
    Игнорирует неизвестные команды, не совпадающие ни с одним проектом.
    """
    msg = update.message
    if not msg:
        return

    text_raw = msg.text or ""
    parts    = text_raw.split()
    if not parts:
        return

    # Извлекаем команду без слэша и без @botname
    cmd_part = parts[0].lstrip("/").split("@")[0].lower()

    # Список зарезервированных команд — не обрабатываем
    RESERVED = {
        "done", "inprogress", "review", "final", "freeze",
        "deadline", "assign", "waiting", "report", "help",
        "start", "topicid", "task", "s", "test", "setup",
        "d", "ip", "r", "f", "t",  # короткие алиасы
    }
    if cmd_part in RESERVED:
        return

    # Разбираем: /авито_25 или /авито 25
    episode_query = ""
    if "_" in cmd_part:
        maybe_proj, maybe_ep = cmd_part.rsplit("_", 1)
        if re.match(r"^[\d.]+$", maybe_ep):
            cmd_part      = maybe_proj
            episode_query = maybe_ep
        # иначе episode_query из следующего аргумента
    if not episode_query and len(parts) > 1:
        episode_query = parts[1]

    project_name = find_project_fuzzy(cmd_part)
    if not project_name:
        return  # молча игнорируем — просто неизвестная команда

    tid  = msg.message_thread_id
    task = find_active_task(project_name, episode_query)
    if not task:
        ep_str = f" · #{escape_md(episode_query)}" if episode_query else ""
        await msg.reply_text(
            f"📭 Активных задач нет: *{escape_md(project_name)}{ep_str}*",
            parse_mode="Markdown",
            message_thread_id=tid,
        )
        return

    await _show_task_card(msg, task, context.bot_data, thread_id=tid)


async def cmd_setup(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    /setup <Название проекта>
    Регистрирует текущий топик как проект в Notion.
    Использование: /setup СОЛНЫШКО
    """
    msg = update.message
    tid = msg.message_thread_id
    chat_id = msg.chat_id

    if not tid:
        await msg.reply_text(
            "⚠️ Эта команда работает только внутри топика (ветки).\n"
            "Перейди в нужный топик и повтори.",
            message_thread_id=None,
        )
        return

    if not context.args:
        await msg.reply_text(
            "📁 Использование: `/setup Название проекта`\n\n"
            "_Пример: /setup СОЛНЫШКО_\n"
            "_Пример: /setup Не мои мысли_\n\n"
            f"Текущий топик ID: `{tid}`",
            parse_mode="Markdown",
            message_thread_id=tid,
        )
        return

    project_name = " ".join(context.args).strip()
    created = create_project(project_name, tid, chat_id)
    if created:
        await msg.reply_text(
            f"✅ Проект *{escape_md(project_name)}* зарегистрирован!\n"
            f"Топик ID: `{tid}`\n\n"
            f"Теперь можно:\n"
            f"• `/task {project_name.lower()} 1` — открыть задачу\n"
            f"• `/s {project_name.lower()} 1 монтаж` — сменить статус",
            parse_mode="Markdown",
            message_thread_id=tid,
        )
        logger.info(f"cmd_setup: registered '{project_name}' thread_id={tid} chat_id={chat_id}")
    else:
        # Проверяем — может уже зарегистрирован
        existing = get_project_name(tid)
        if existing:
            await msg.reply_text(
                f"ℹ️ Топик уже зарегистрирован как *{escape_md(existing)}*.\n"
                f"Если надо изменить название — сделай это вручную в Notion.",
                parse_mode="Markdown",
                message_thread_id=tid,
            )
        else:
            await msg.reply_text(
                "❌ Ошибка при создании проекта. Проверь подключение к Notion.",
                message_thread_id=tid,
            )


async def cmd_help(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    tid = update.message.message_thread_id
    await update.message.reply_text(
        "🎬 *PostProd Bot — справка*\n\n"
        "*Создание задачи:*\n"
        "Напиши сообщение с `#выпуск` и `@исполнитель` в ветке проекта\n\n"
        "*Найти задачу по проекту + выпуску:*\n"
        "`/task авито 25` — карточка с кнопками статусов\n"
        "`/авито 25` — то же самое, сокращённо\n\n"
        "*Сменить статус без reply:*\n"
        "`/s авито 25 монтаж`\n"
        "`/s авито 25 сдано`\n"
        "Статусы: `монтаж` · `правки` · `финал` · `клиент`\n"
        "         `согласование` · `сдано` · `стоп`\n"
        "         `цвет` · `звук` · `графика`\n\n"
        "*Статусы через reply* (ответ на сообщение задачи):\n"
        "`/inprogress` — ✂️ Монтаж\n"
        "`/review` — 🔄 Правки _(+1 итерация)_\n"
        "`/final` — 👁️ Финальный просмотр\n"
        "`/done` — ✅ Сдано\n"
        "`/freeze` — ⏸️ Заморожено\n\n"
        "*Управление* (reply на задачу):\n"
        "`/deadline 25 марта` — поставить дедлайн\n"
        "`/assign @kate` — назначить исполнителя\n"
        "`/waiting цвет` — ждём внешний отдел\n\n"
        "`/report` — 📊 дайджест задач",
        parse_mode="Markdown",
        message_thread_id=tid,
    )


# ─────────────────────────────────────────────────────────────────────────────
# Диагностика
# ─────────────────────────────────────────────────────────────────────────────
async def cmd_test(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    /test — проверяет соединение с OpenAI и Notion.
    Выводит статус каждого сервиса прямо в чат.
    """
    msg = update.message
    tid = msg.message_thread_id
    await msg.reply_text("🔍 Проверяю соединения...", message_thread_id=tid)

    lines = []

    # ── OpenAI ──────────────────────────────────────────────────────────────
    try:
        resp = await openai_client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[{"role": "user", "content": "Ответь одним словом: работаю"}],
            max_tokens=10,
            temperature=0,
        )
        answer = resp.choices[0].message.content.strip()
        lines.append(f"✅ *OpenAI* — ок (`{escape_md(answer)}`)")
    except Exception as e:
        lines.append(f"❌ *OpenAI* — ошибка:\n`{escape_md(str(e)[:200])}`")

    # ── Notion — Задачи ──────────────────────────────────────────────────────
    try:
        res = notion.databases.retrieve(database_id=TASKS_DB)
        db_name = res.get("title", [{}])[0].get("plain_text", TASKS_DB[:8])
        lines.append(f"✅ *Notion / Задачи* — ок (`{escape_md(db_name)}`)")
    except Exception as e:
        lines.append(f"❌ *Notion / Задачи* — ошибка:\n`{escape_md(str(e)[:200])}`")

    # ── Notion — Проекты ────────────────────────────────────────────────────
    try:
        res = notion.databases.retrieve(database_id=PROJECTS_DB)
        db_name = res.get("title", [{}])[0].get("plain_text", PROJECTS_DB[:8])
        lines.append(f"✅ *Notion / Проекты* — ок (`{escape_md(db_name)}`)")
    except Exception as e:
        lines.append(f"❌ *Notion / Проекты* — ошибка:\n`{escape_md(str(e)[:200])}`")

    # ── Notion — Дневник ────────────────────────────────────────────────────
    try:
        res = notion.databases.retrieve(database_id=DIARY_DB)
        db_name = res.get("title", [{}])[0].get("plain_text", DIARY_DB[:8])
        lines.append(f"✅ *Notion / Дневник* — ок (`{escape_md(db_name)}`)")
    except Exception as e:
        lines.append(f"❌ *Notion / Дневник* — ошибка:\n`{escape_md(str(e)[:200])}`")

    # ── Notion — Отчёты ─────────────────────────────────────────────────────
    try:
        res = notion.databases.retrieve(database_id=REPORTS_DB)
        db_name = res.get("title", [{}])[0].get("plain_text", REPORTS_DB[:8])
        lines.append(f"✅ *Notion / Отчёты* — ок (`{escape_md(db_name)}`)")
    except Exception as e:
        lines.append(f"❌ *Notion / Отчёты* — ошибка:\n`{escape_md(str(e)[:200])}`")

    await msg.reply_text(
        "🛠 *Диагностика*\n\n" + "\n".join(lines),
        parse_mode="Markdown",
        message_thread_id=tid,
    )


# ─────────────────────────────────────────────────────────────────────────────
# Scheduled jobs
# ─────────────────────────────────────────────────────────────────────────────
async def _build_digest_text(title: str) -> str:
    sections = [
        ("🆕 Новые",       "🆕 Новая"),
        ("✂️ В монтаже",   "✂️ Монтаж"),
        ("🔄 Правки",      "🔄 Правки"),
        ("🎨 Ждём цвет",   "🎨 Ждём цвет"),
        ("🔊 Ждём звук",   "🔊 Ждём звук"),
        ("✨ Ждём графику", "✨ Ждём графику"),
    ]
    lines = [title + "\n"]
    total = 0
    for label, status in sections:
        tasks = get_tasks_by_status(status)
        if tasks:
            lines.append(f"\n{label} — {len(tasks)}")
            for t in tasks[:5]:
                lines.append(fmt_task_line(t))
            total += len(tasks)

    due_today = get_due_soon(days=0)
    if due_today:
        lines.append(f"\n⏰ *Дедлайн сегодня* — {len(due_today)}")
        for t in due_today:
            lines.append(fmt_deadline_line(t))

    overdue = get_overdue()
    if overdue:
        lines.append(f"\n🔥 *Просрочено* — {len(overdue)}")
        for t in overdue[:4]:
            lines.append(fmt_deadline_line(t))

    if not total and not due_today and not overdue:
        lines.append("🎉 Все задачи завершены!")

    return "\n".join(lines)


async def job_morning_digest(context: ContextTypes.DEFAULT_TYPE) -> None:
    chats = _all_digest_chats(context.bot_data)
    if not chats:
        return
    msg = await _build_digest_text(f"☀️ *Доброе утро! Дайджест на {datetime.now().strftime('%d.%m.%Y')}*")
    for chat_id in chats:
        try:
            await context.bot.send_message(chat_id=chat_id, text=msg,
                                           parse_mode="Markdown", disable_web_page_preview=True)
        except Exception as e:
            logger.error(f"Morning digest send error {chat_id}: {e}")


async def job_deadline_check(context: ContextTypes.DEFAULT_TYPE) -> None:
    today = date.today()

    # Просроченные задачи (дедлайн уже прошёл)
    try:
        overdue_res = notion.databases.query(
            database_id=TASKS_DB,
            filter={"and": [
                {"property": "Дедлайн", "date": {"before": today.isoformat()}},
                {"property": "Статус", "select": {"does_not_equal": "✅ Сдано"}},
                {"property": "Статус", "select": {"does_not_equal": "❌ Отменено"}},
                {"property": "Статус", "select": {"does_not_equal": "⏸️ Заморожено"}},
            ]},
            page_size=20,
        )
        overdue = overdue_res.get("results", [])
    except Exception as e:
        logger.error(f"overdue query: {e}")
        overdue = []

    # Задачи с дедлайном в ближайшие 24 часа
    due_soon = get_due_soon(days=1)

    # Пингуем прямо в топике задачи
    async def ping_in_topic(task: dict, prefix: str) -> None:
        p = task["properties"]
        name     = _get_title(p)[:60] or "Задача"
        assignee = _get_rich(p, "Исполнитель")
        tg_url   = p.get("Ссылка TG", {}).get("url", "")
        dl       = (p.get("Дедлайн") or {}).get("date", {})
        dl_str   = dl.get("start", "—") if isinstance(dl, dict) else "—"
        chat_id_raw = (p.get("TG Chat ID") or {}).get("number")
        thread_id_raw = (p.get("TG Thread ID") or {}).get("number")

        mention = assignee if assignee and assignee != "—" else ""
        text = (
            f"{prefix}\n"
            f"*{escape_md(name)}*\n"
            f"📅 Дедлайн: {escape_md(dl_str)}\n"
            + (f"👤 {escape_md(mention)}\n" if mention else "")
            + (f"[→ Сообщение]({tg_url})" if tg_url else "")
        )

        sent = False
        # Пробуем отправить в конкретный топик
        if chat_id_raw and thread_id_raw:
            try:
                await context.bot.send_message(
                    chat_id=int(chat_id_raw),
                    message_thread_id=int(thread_id_raw),
                    text=text,
                    parse_mode="Markdown",
                    disable_web_page_preview=True,
                )
                sent = True
            except Exception as e:
                logger.warning(f"ping_in_topic direct failed: {e}")

        # Fallback — шлём в общий дайджест-чат
        if not sent:
            for cid in _all_digest_chats(context.bot_data):
                try:
                    await context.bot.send_message(
                        chat_id=cid, text=text,
                        parse_mode="Markdown", disable_web_page_preview=True,
                    )
                except Exception as e:
                    logger.error(f"ping fallback {cid}: {e}")

    for task in overdue:
        await ping_in_topic(task, "🚨 *ПРОСРОЧЕНО!*")

    for task in due_soon:
        # Не дублируем если уже в просроченных
        tid = task["id"]
        if any(t["id"] == tid for t in overdue):
            continue
        await ping_in_topic(task, "⏰ *Дедлайн через 24 часа*")


async def job_waiting_check(context: ContextTypes.DEFAULT_TYPE) -> None:
    chats = _all_digest_chats(context.bot_data)
    if not chats:
        return
    stale = get_waiting_stale()
    if not stale:
        return
    lines = [f"⚠️ *Ожидание >{WAITING_ALERT_DAYS} дней — нет ответа:*"]
    for t in stale:
        p = t["properties"]
        name  = _get_title(p)[:50] or "?"
        dept  = _get_select(p, "Ожидаем отдел")
        wd    = p.get("Ожидание с", {}).get("date") or {}
        since = wd.get("start", "?")
        lines.append(f"  • {name} | {dept} | с {since}")
    msg = "\n".join(lines)
    for chat_id in chats:
        try:
            await context.bot.send_message(chat_id=chat_id, text=msg, parse_mode="Markdown")
        except Exception as e:
            logger.error(f"Waiting check send error {chat_id}: {e}")


async def handle_new_topic(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Автоматически создаёт проект в Notion при создании нового топика в группе."""
    message = update.message
    if not message or not message.forum_topic_created:
        return
    topic_name = message.forum_topic_created.name
    thread_id  = message.message_thread_id
    chat_id    = message.chat_id
    created    = create_project(topic_name, thread_id, chat_id)
    if created:
        await message.reply_text(
            f"📁 Проект *{escape_md(topic_name)}* добавлен в Notion\n"
            f"Топик ID: `{thread_id}`",
            parse_mode="Markdown",
            message_thread_id=thread_id,
        )
        logger.info(f"Auto-created project: {topic_name} (thread_id={thread_id})")


# ─────────────────────────────────────────────────────────────────────────────
# Добавление бота в новый чат
# ─────────────────────────────────────────────────────────────────────────────
async def handle_bot_added(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    Срабатывает при изменении статуса бота в группе (MY_CHAT_MEMBER):
    - При добавлении: сохраняем chat_id, отправляем приветствие
    - При удалении/бане: убираем chat_id из списка рассылки
    """
    result = update.my_chat_member
    if not result:
        return

    old_status = result.old_chat_member.status
    new_status  = result.new_chat_member.status
    chat_id     = result.chat.id
    chat_title  = result.chat.title or "чат"

    # ── Бот удалён / забанен → убираем из списка дайджеста ──────────────────
    if new_status in ("left", "kicked"):
        chats: set = context.bot_data.get("registered_chats", set())
        chats.discard(chat_id)
        context.bot_data["registered_chats"] = chats
        logger.info(f"Bot removed from chat {chat_id} ({chat_title}), unregistered")
        return

    # ── Бот добавлен (был kicked/left → стал member/administrator) ───────────
    if old_status not in ("left", "kicked") or new_status not in ("member", "administrator"):
        return

    # Сохраняем chat_id в bot_data для дайджестов
    chats: set = context.bot_data.setdefault("registered_chats", set())
    chats.add(chat_id)
    logger.info(f"Bot added to chat {chat_id} ({chat_title}), registered for digest")

    is_forum = getattr(result.chat, "is_forum", False)

    if is_forum:
        text = (
            f"👋 Привет, *{escape_md(chat_title)}*!\n\n"
            f"Я бот управления задачами пост-продакшена.\n\n"
            f"*Как зарегистрировать топики:*\n"
            f"Напиши что-нибудь в каждом топике — я авторегистрирую его.\n"
            f"Или вручную: `/setup Название` в нужном топике.\n\n"
            f"После регистрации доступны команды:\n"
            f"`/t <проект> <выпуск>` — карточка задачи\n"
            f"`/s <проект> <выпуск> <статус>` — сменить статус\n\n"
            f"📖 /help — полная справка\n\n"
            f"_Ежедневный дайджест будет приходить сюда в {DIGEST_HOUR}:00_"
        )
    else:
        text = (
            f"👋 Привет, *{escape_md(chat_title)}*!\n\n"
            f"Я PostProd бот для управления задачами монтажа.\n"
            f"_Ежедневный дайджест будет приходить сюда в {DIGEST_HOUR}:00_\n\n"
            f"📖 /help — справка по командам"
        )

    try:
        await context.bot.send_message(
            chat_id=chat_id,
            text=text,
            parse_mode="Markdown",
        )
        logger.info(f"Welcome message sent to {chat_id} ({chat_title})")
    except Exception as e:
        logger.error(f"handle_bot_added send: {e}")


# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────
def main() -> None:
    app = Application.builder().token(TELEGRAM_TOKEN).build()

    # Добавление бота в чат — приветствие + инструкция по регистрации топиков
    app.add_handler(ChatMemberHandler(handle_bot_added, ChatMemberHandler.MY_CHAT_MEMBER))

    # Сообщения
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))
    app.add_handler(MessageHandler(filters.VOICE, handle_message))
    app.add_handler(MessageHandler(filters.StatusUpdate.FORUM_TOPIC_CREATED, handle_new_topic))
    app.add_handler(CallbackQueryHandler(handle_callback))

    # Статусы (полные команды)
    app.add_handler(CommandHandler("done",       cmd_done))
    app.add_handler(CommandHandler("inprogress", cmd_inprogress))
    app.add_handler(CommandHandler("review",     cmd_review))
    app.add_handler(CommandHandler("final",      cmd_final))
    app.add_handler(CommandHandler("freeze",     cmd_freeze))

    # Короткие алиасы статусов
    app.add_handler(CommandHandler("d",  cmd_d))   # /d  → сдано
    app.add_handler(CommandHandler("ip", cmd_ip))  # /ip → монтаж
    app.add_handler(CommandHandler("r",  cmd_r))   # /r  → правки
    app.add_handler(CommandHandler("f",  cmd_f))   # /f  → финал

    # Управление
    app.add_handler(CommandHandler("deadline", cmd_deadline))
    app.add_handler(CommandHandler("assign",   cmd_assign))
    app.add_handler(CommandHandler("waiting",  cmd_waiting))

    # UX-команды (по проекту + выпуску)
    app.add_handler(CommandHandler("task",  cmd_task))
    app.add_handler(CommandHandler("t",     cmd_task))   # /t = короткий алиас /task
    app.add_handler(CommandHandler("s",     cmd_status_ref))
    app.add_handler(CommandHandler("setup", cmd_setup))

    # Инфо
    app.add_handler(CommandHandler("report",  cmd_report))
    app.add_handler(CommandHandler("help",    cmd_help))
    app.add_handler(CommandHandler("start",   cmd_help))
    app.add_handler(CommandHandler("topicid", cmd_topicid))
    app.add_handler(CommandHandler("test",    cmd_test))

    # Умные команды /авито 25 — перехватывает всё остальное
    # (должно быть ПОСЛЕДНИМ, чтобы не перекрывать специфические команды)
    app.add_handler(MessageHandler(filters.COMMAND, cmd_smart_project))

    # Расписание
    jq: JobQueue = app.job_queue
    jq.run_daily(job_morning_digest, time=time(hour=DIGEST_HOUR, minute=0))
    jq.run_daily(job_daily_report,   time=time(hour=DIGEST_HOUR - 1, minute=50))  # в 8:50 — до дайджеста
    jq.run_repeating(job_deadline_check, interval=21600, first=300)  # каждые 6 часов
    jq.run_repeating(job_waiting_check,  interval=21600, first=600)  # каждые 6 часов

    logger.info("🚀 PostProd Bot v2.0 started!")
    app.run_polling(drop_pending_updates=True)


if __name__ == "__main__":
    main()
