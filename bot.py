"""
🎬 PostProd Task Bot — v2.0
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

Команды (reply на сообщение задачи):
  /inprogress   ✂️ В монтаже
  /review       🔄 Правки (+1 итерация)
  /final        👁️ Финальный просмотр
  /done         ✅ Сдано
  /freeze       ⏸️ Заморожено
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
import re
from datetime import date, datetime, time, timedelta
from typing import Optional

from notion_client import Client as NotionClient
from openai import AsyncOpenAI
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.ext import (
    Application,
    CallbackQueryHandler,
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
- status (string): один из статусов [🆕 Новая, ✂️ Монтаж, 🔄 Правки, 👁️ Финальный просмотр, 🎨 Ждём цвет, 🔊 Ждём звук, ✨ Ждём графику, ✅ Сдано, ⏸️ Заморожено]
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
    ep_str   = f" · #{episode}" if episode and episode != "—" else ""
    link     = f" [→]({tg_url})" if tg_url else ""
    return f"  • {name} | {project}{ep_str} | {assignee}{link}"


def fmt_deadline_line(task: dict) -> str:
    p = task["properties"]
    name     = _get_title(p)[:50] or "Без названия"
    dl       = p.get("Дедлайн", {}).get("date") or {}
    deadline = dl.get("start", "?")
    assignee = _get_rich(p, "Исполнитель")
    project  = _get_select(p, "Проект")
    return f"  • {name} | {project} | {assignee} | 📅 {deadline}"


# ─────────────────────────────────────────────────────────────────────────────
# Обработчик сообщений
# ─────────────────────────────────────────────────────────────────────────────
async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    message = update.message
    if not message:
        return

    thread_id = message.message_thread_id

    # Определяем название топика через Notion (по числовому ID)
    topic = "Общий"
    if message.is_topic_message and thread_id:
        fc = getattr(message, "forum_topic_created", None)
        if fc:
            topic = fc.name
        else:
            # Ищем проект по Топик ID в Notion
            notion_name = get_project_name(thread_id)
            topic = notion_name if notion_name else f"Топик-{thread_id}"

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
    else:
        await msg.reply_text("❓ Задача не найдена в базе", message_thread_id=tid)


async def cmd_done(u, c):       await _set_status(u, c, "✅ Сдано",                "✅")
async def cmd_inprogress(u, c): await _set_status(u, c, "✂️ Монтаж",              "✂️")
async def cmd_review(u, c):     await _set_status(u, c, "🔄 Правки",              "🔄", increment_iter=True)
async def cmd_final(u, c):      await _set_status(u, c, "👁️ Финальный просмотр", "👁️")
async def cmd_freeze(u, c):     await _set_status(u, c, "⏸️ Заморожено",          "⏸️")


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


async def cmd_help(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    tid = update.message.message_thread_id
    await update.message.reply_text(
        "🎬 *PostProd Bot — справка*\n\n"
        "*Создание задачи:*\n"
        "Напиши сообщение с `#выпуск` и `@исполнитель` в ветке проекта\n\n"
        "*Статусы* (reply на сообщение задачи):\n"
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
    if not DIGEST_CHAT_IDS:
        return
    msg = await _build_digest_text(f"☀️ *Доброе утро! Дайджест на {datetime.now().strftime('%d.%m.%Y')}*")
    for chat_id in DIGEST_CHAT_IDS:
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
            for cid in DIGEST_CHAT_IDS:
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
    if not DIGEST_CHAT_IDS:
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
    for chat_id in DIGEST_CHAT_IDS:
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
            f"📁 Проект *{topic_name}* добавлен в Notion\n"
            f"Топик ID: `{thread_id}`",
            parse_mode="Markdown",
            message_thread_id=thread_id,
        )
        logger.info(f"Auto-created project: {topic_name} (thread_id={thread_id})")


# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────
def main() -> None:
    app = Application.builder().token(TELEGRAM_TOKEN).build()

    # Сообщения
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))
    app.add_handler(MessageHandler(filters.VOICE, handle_message))
    app.add_handler(MessageHandler(filters.StatusUpdate.FORUM_TOPIC_CREATED, handle_new_topic))
    app.add_handler(CallbackQueryHandler(handle_callback))

    # Статусы
    app.add_handler(CommandHandler("done",       cmd_done))
    app.add_handler(CommandHandler("inprogress", cmd_inprogress))
    app.add_handler(CommandHandler("review",     cmd_review))
    app.add_handler(CommandHandler("final",      cmd_final))
    app.add_handler(CommandHandler("freeze",     cmd_freeze))

    # Управление
    app.add_handler(CommandHandler("deadline", cmd_deadline))
    app.add_handler(CommandHandler("assign",   cmd_assign))
    app.add_handler(CommandHandler("waiting",  cmd_waiting))

    # Инфо
    app.add_handler(CommandHandler("report",  cmd_report))
    app.add_handler(CommandHandler("help",    cmd_help))
    app.add_handler(CommandHandler("start",   cmd_help))
    app.add_handler(CommandHandler("topicid", cmd_topicid))

    # Расписание
    jq: JobQueue = app.job_queue
    jq.run_daily(job_morning_digest, time=time(hour=DIGEST_HOUR, minute=0))
    jq.run_repeating(job_deadline_check, interval=21600, first=300)  # каждые 6 часов
    jq.run_repeating(job_waiting_check,  interval=21600, first=600)  # каждые 6 часов

    logger.info("🚀 PostProd Bot v2.0 started!")
    app.run_polling(drop_pending_updates=True)


if __name__ == "__main__":
    main()
