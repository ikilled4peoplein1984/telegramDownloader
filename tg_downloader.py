#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Telegram Channel Downloader — финальная версия с рабочим пулом сессий.

Ключевое отличие от предыдущей: build_client_pool использует ОТДЕЛЬНЫЕ
файлы сессий (tg_session_pool1.session, ...), поэтому конфликта с main-сессией
(tg_session.session) нет и ошибка "database is locked" исчезает.
"""

import asyncio
import importlib.util
import json
import math
import os
import re
import shutil
import string
import sys
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

try:
    from telethon import TelegramClient, errors
    from rich.console import Console
    from rich.markup import escape
    from rich.progress import (
        BarColumn, DownloadColumn, MofNCompleteColumn, Progress, SpinnerColumn,
        TaskProgressColumn, TextColumn, TimeRemainingColumn, TransferSpeedColumn,
    )
    from rich.table import Table
except ImportError:
    print("Не хватает библиотек. Установите:  pip install telethon rich cryptg")
    sys.exit(1)

BASE_DIR = Path(__file__).resolve().parent
CONFIG_FILE = BASE_DIR / "config.json"
SESSION_FILE = str(BASE_DIR / "tg_session")
PAGE_SIZE = 30
ARCHIVE_EXT = {"zip", "rar", "7z", "tar", "gz", "bz2", "xz", "tgz", "iso", "zst"}

REQUEST_SIZE = 512 * 1024
MULTIPART_MIN = 20 * 1024 * 1024
DEFAULT_WORKERS = 2
DEFAULT_PARTS = 4
DEFAULT_POOL = 4
MAX_CONCURRENT_REQUESTS = 12

console = Console()


# ───────────────────────────── утилиты ─────────────────────────────

def human(n: int) -> str:
    if not n:
        return "?"
    n = float(n)
    for unit in ("Б", "КБ", "МБ", "ГБ", "ТБ"):
        if n < 1024 or unit == "ТБ":
            return f"{n:.0f} {unit}" if unit == "Б" else f"{n:.1f} {unit}"
        n /= 1024


def safe_filename(name: str) -> str:
    name = re.sub(r'[<>:"/\\|?*\x00-\x1f]', "_", name).strip().rstrip(".")
    return name or "file"


def short(s: str, n: int = 45) -> str:
    return s if len(s) <= n else s[: n - 1] + "…"


def load_config() -> dict:
    if CONFIG_FILE.exists():
        try:
            return json.loads(CONFIG_FILE.read_text(encoding="utf-8"))
        except Exception:
            pass
    return {}


def save_config(cfg: dict) -> None:
    CONFIG_FILE.write_text(json.dumps(cfg, ensure_ascii=False, indent=2), encoding="utf-8")


def ask_int(prompt: str, default: int, lo: int, hi: int) -> int:
    while True:
        s = console.input(f"{prompt} [dim]({lo}-{hi}, Enter = {default})[/]: ").strip()
        if not s:
            return default
        if s.isdigit() and lo <= int(s) <= hi:
            return int(s)
        console.print(f"[red]Введите число от {lo} до {hi}.[/]")


def check_cryptg() -> None:
    if importlib.util.find_spec("cryptg") is not None:
        console.print("[green]✓ cryptg установлен[/] — быстрое шифрование включено.\n")
    else:
        console.print(
            "[bold yellow]⚠ cryptg НЕ установлен.[/] Без него скорость ограничена CPU.\n"
            "  Исправьте: [cyan]pip install cryptg[/]\n"
        )


@dataclass
class Item:
    msg: object
    msg_id: int
    name: str
    safe_name: str
    size: int
    date: datetime
    kind: str


# ───────────────────────────── вход / пул ─────────────────────────────

async def login(cfg: dict) -> TelegramClient:
    """Главный клиент — своя сессия tg_session.session."""
    if not cfg.get("api_id") or not cfg.get("api_hash"):
        console.print("[bold]Первый запуск.[/] Получите api_id и api_hash на https://my.telegram.org")
        while True:
            try:
                cfg["api_id"] = int(console.input("api_id: ").strip())
                break
            except ValueError:
                console.print("[red]api_id должен быть числом.[/]")
        while True:
            h = console.input("api_hash: ").strip()
            if h:
                cfg["api_hash"] = h
                break
            console.print("[red]api_hash не может быть пустым.[/]")
        save_config(cfg)

    client = TelegramClient(SESSION_FILE, cfg["api_id"], cfg["api_hash"])
    await client.start()
    me = await client.get_me()
    console.print(f"[green]✓ Вошли как[/] {escape(me.first_name or '')} (@{me.username or '—'})")
    check_cryptg()
    return client


async def build_client_pool(cfg: dict, size: int) -> list:
    """
    Пул клиентов для СКАЧИВАНИЯ. Каждый — своя сессия tg_session_poolN.session.
    Главная сессия (tg_session.session) сюда НЕ входит — конфликта не будет.
    """
    size = max(1, size)
    console.print(f"[cyan]Создаю пул из {size} MTProto-соединений…[/]")
    pool = []
    for i in range(1, size + 1):
        session_path = f"{SESSION_FILE}_pool{i}"
        try:
            c = TelegramClient(session_path, cfg["api_id"], cfg["api_hash"])
            await c.start()
            await c.get_me()
            pool.append(c)
            console.print(f"[dim]  ✓ соединение #{i} готово[/]")
        except Exception as e:
            console.print(f"[yellow]  ✗ соединение #{i} не создано: {escape(str(e))}[/]")
    if not pool:
        raise RuntimeError("Ни одно соединение не создалось")
    console.print(f"[green]✓ Активных соединений:[/] {len(pool)}\n")
    return pool


async def close_pool(pool: list) -> None:
    for c in pool:
        try:
            await c.disconnect()
        except Exception:
            pass


# ───────────────────────────── выбор канала ─────────────────────────────

async def pick_entity(client: TelegramClient):
    console.print(
        "Укажите канал: [cyan]@username[/], ссылку [cyan]t.me/...[/], числовой ID, "
        "либо [cyan]Enter[/] — выбрать из ваших чатов."
    )
    while True:
        s = console.input("[bold]Канал:[/] ").strip()
        if not s:
            ent = await pick_from_dialogs(client)
            if ent:
                return ent
            continue
        try:
            m = re.search(r"t\.me/c/(\d+)", s)
            if m:
                target = int("-100" + m.group(1))
            elif re.fullmatch(r"-?\d+", s):
                target = int(s)
            else:
                target = s
            if isinstance(target, int):
                await client.get_dialogs()
            return await client.get_entity(target)
        except Exception as e:
            console.print(f"[red]Не удалось найти канал:[/] {escape(str(e))}")


async def pick_from_dialogs(client: TelegramClient):
    with console.status("Загружаю список ваших каналов и групп…"):
        dialogs = [d async for d in client.iter_dialogs() if d.is_channel or d.is_group]
    query = ""
    while True:
        shown = [d for d in dialogs if query.lower() in (d.name or "").lower()][:50]
        t = Table(show_header=True, header_style="bold")
        t.add_column("#", justify="right")
        t.add_column("Название")
        t.add_column("Тип")
        for i, d in enumerate(shown, 1):
            t.add_row(str(i), escape(d.name or "—"),
                      "канал" if d.is_channel and not d.is_group else "группа")
        console.print(t)
        console.print("Введите [cyan]номер[/], [cyan]текст[/] для поиска, [cyan]q[/] — назад.")
        s = console.input("> ").strip()
        if s.lower() == "q":
            return None
        if s.isdigit() and 1 <= int(s) <= len(shown):
            return shown[int(s) - 1].entity
        query = s


# ───────────────────────────── сканирование ─────────────────────────────

async def scan_channel(client: TelegramClient, entity, include_photos: bool) -> list:
    items = []
    total = (await client.get_messages(entity, limit=0)).total or None
    progress = Progress(
        SpinnerColumn(), TextColumn("[bold]Сканирую сообщения"), BarColumn(),
        MofNCompleteColumn(), TextColumn("найдено файлов: {task.fields[found]}"),
        console=console,
    )
    with progress:
        task = progress.add_task("scan", total=total, found=0)
        async for m in client.iter_messages(entity):
            progress.advance(task)
            if not m.media or m.sticker:
                continue
            if m.photo:
                if not include_photos:
                    continue
                name, kind, size = f"photo_{m.id}.jpg", "фото", (m.file.size if m.file else 0) or 0
            elif m.document and m.file:
                ext = (m.file.ext or "").lower()
                if m.video:
                    kind = "видео"
                elif m.audio or m.voice:
                    kind = "аудио"
                elif ext.lstrip(".") in ARCHIVE_EXT:
                    kind = "архив"
                else:
                    kind = "файл"
                name = m.file.name or f"{kind}_{m.id}{ext}"
                size = m.file.size or 0
            else:
                continue
            items.append(Item(m, m.id, name, safe_filename(name), size, m.date, kind))
            progress.update(task, found=len(items))
    return items


# ───────────────────────────── выбор файлов ─────────────────────────────

def parse_numbers(s: str, maxn: int) -> set:
    res = set()
    for tok in re.split(r"[,\s]+", s.strip()):
        if not tok:
            continue
        m = re.fullmatch(r"(\d+)-(\d+)", tok)
        if m:
            a, b = sorted((int(m.group(1)), int(m.group(2))))
            res.update(range(max(a, 1), min(b, maxn) + 1))
        elif tok.isdigit():
            if 1 <= int(tok) <= maxn:
                res.add(int(tok))
        else:
            raise ValueError(tok)
    return res


def choose_files(items: list):
    selected = set()
    page = 0
    while True:
        pages = max(1, (len(items) + PAGE_SIZE - 1) // PAGE_SIZE)
        page = max(0, min(page, pages - 1))
        chunk = items[page * PAGE_SIZE:(page + 1) * PAGE_SIZE]

        t = Table(title=f"Файлов: {len(items)} | Страница {page + 1}/{pages}", header_style="bold")
        t.add_column("#", justify="right")
        t.add_column("✓")
        t.add_column("Тип")
        t.add_column("Имя")
        t.add_column("Размер", justify="right")
        t.add_column("Дата")
        for i, it in enumerate(chunk, page * PAGE_SIZE + 1):
            mark = "[green]✔[/]" if it.msg_id in selected else " "
            t.add_row(str(i), mark, it.kind, escape(short(it.name, 60)),
                      human(it.size), it.date.strftime("%Y-%m-%d"))
        console.print(t)
        sel_size = sum(i.size for i in items if i.msg_id in selected)
        console.print(f"[bold]Выбрано:[/] {len(selected)} шт., {human(sel_size)}")
        console.print(
            "[dim]Команды: [/]"
            "[cyan]all[/] | [cyan]none[/] | [cyan]1,5,10-20[/] | [cyan]-3,7-9[/] | "
            "[cyan]ext zip rar[/] | [cyan]find текст[/] | [cyan]sort name|size|date[/] | "
            "[cyan]n/p[/] | [cyan]page N[/] | [cyan]go[/] | [cyan]q[/]"
        )
        cmd = console.input("[bold]> [/]").strip()
        low = cmd.lower()
        try:
            if low in ("", "n"):
                page = (page + 1) % pages
            elif low == "p":
                page = (page - 1) % pages
            elif low.startswith("page "):
                page = int(low.split()[1]) - 1
            elif low in ("all", "a", "все"):
                selected = {i.msg_id for i in items}
            elif low == "none":
                selected.clear()
            elif low == "q":
                return None
            elif low == "go":
                if not selected:
                    console.print("[yellow]Ничего не выбрано.[/]")
                    continue
                return [i for i in items if i.msg_id in selected]
            elif low.startswith("ext "):
                exts = {e.strip(".").lower() for e in low.split()[1:]}
                selected |= {i.msg_id for i in items
                             if Path(i.name).suffix.lstrip(".").lower() in exts}
            elif low.startswith("find "):
                needle = low[5:].strip()
                selected |= {i.msg_id for i in items if needle in i.name.lower()}
            elif low.startswith("sort "):
                key = low.split()[1]
                if key == "name":
                    items.sort(key=lambda i: i.name.lower())
                elif key == "size":
                    items.sort(key=lambda i: i.size, reverse=True)
                elif key == "date":
                    items.sort(key=lambda i: i.date, reverse=True)
                page = 0
            elif low.startswith("-"):
                for n in parse_numbers(low[1:], len(items)):
                    selected.discard(items[n - 1].msg_id)
            else:
                for n in parse_numbers(low, len(items)):
                    selected.add(items[n - 1].msg_id)
        except (ValueError, IndexError):
            console.print("[red]Не понял команду.[/]")


# ───────────────────────────── путь и настройки ─────────────────────────────

def show_drives():
    if os.name != "nt":
        return
    parts = []
    for c in string.ascii_uppercase:
        root = f"{c}:\\"
        if os.path.exists(root):
            try:
                parts.append(f"{c}: (свободно {human(shutil.disk_usage(root).free)})")
            except OSError:
                pass
    if parts:
        console.print("[dim]Доступные диски: " + " | ".join(parts) + "[/]")


def ask_dest(cfg: dict) -> Path:
    default = cfg.get("download_dir", str(Path.home() / "Downloads" / "Telegram"))
    show_drives()
    while True:
        s = console.input(f"[bold]Папка[/] [dim](Enter = {escape(default)})[/]: ").strip().strip('"')
        p = Path(os.path.expanduser(s or default))
        try:
            p.mkdir(parents=True, exist_ok=True)
            test = p / ".write_test"
            test.write_text("ok")
            test.unlink()
            cfg["download_dir"] = str(p)
            save_config(cfg)
            return p
        except Exception as e:
            console.print(f"[red]Нельзя писать в папку:[/] {escape(str(e))}")


def ask_speed_settings(cfg: dict, pool_size: int):
    console.print("\n[bold]Настройки скорости[/] [dim](больше = быстрее)[/]")
    workers = ask_int("Файлов одновременно", cfg.get("workers", DEFAULT_WORKERS), 1, 8)
    parts = ask_int(f"Потоков на один файл (от {human(MULTIPART_MIN)}; 1 = выкл.)",
                    cfg.get("parts", DEFAULT_PARTS), 1, 8)
    total = workers * parts
    if total > pool_size:
        console.print(f"[yellow]workers × parts = {total} > пула {pool_size}. "
                      f"Часть соединений будет переиспользована.[/]")
    if total > MAX_CONCURRENT_REQUESTS:
        console.print(f"[yellow]Суммарно {total} запросов — возможны FloodWait.[/]")
    cfg["workers"], cfg["parts"] = workers, parts
    save_config(cfg)
    return workers, parts


# ───────────────────────────── скачивание ─────────────────────────────

def build_queue(selected: list, dest: Path):
    existing = {f.name.lower() for f in dest.iterdir()
                if f.is_file() and not f.name.endswith(".part")}
    seen, queue, skipped = set(), [], []
    for it in selected:
        key = it.safe_name.lower()
        if key in existing:
            skipped.append((it, "уже есть в папке"))
        elif key in seen:
            skipped.append((it, "дубликат в списке"))
        else:
            seen.add(key)
            queue.append(it)
    return queue, skipped


class Tracker:
    def __init__(self, progress, overall, task):
        self.progress, self.overall, self.task = progress, overall, task
        self.done = 0

    def add(self, n: int):
        self.done += n
        self.progress.update(self.task, advance=n)
        self.progress.update(self.overall, advance=n)

    def set_abs(self, cur: int):
        self.add(cur - self.done)

    def reset(self):
        self.progress.update(self.overall, advance=-self.done)
        self.progress.update(self.task, completed=0)
        self.done = 0


async def download_multipart(client: TelegramClient, it: Item, tmp: Path, parts: int,
                              tracker: Tracker, sem: asyncio.Semaphore):
    size = it.size
    total_chunks = math.ceil(size / REQUEST_SIZE)
    parts = max(1, min(parts, total_chunks))
    per_part = math.ceil(total_chunks / parts)

    with open(tmp, "wb") as f:
        f.truncate(size)

    async def run_part(idx: int):
        first = idx * per_part
        limit = min(per_part, total_chunks - first)
        if limit <= 0:
            return
        offset = first * REQUEST_SIZE
        async with sem:
            attempt = 0
            while True:
                attempt += 1
                try:
                    with open(tmp, "r+b") as f:
                        f.seek(offset)
                        async for chunk in client.iter_download(
                            it.msg.document, offset=offset, limit=limit,
                            request_size=REQUEST_SIZE, file_size=size,
                        ):
                            f.write(chunk)
                            tracker.add(len(chunk))
                    return
                except errors.FloodWaitError as e:
                    console.print(f"[yellow]FloodWait {e.seconds}s (часть {idx})…[/]")
                    await asyncio.sleep(e.seconds + 2)
                except (ConnectionError, OSError, asyncio.TimeoutError):
                    if attempt >= 4:
                        raise
                    await asyncio.sleep(1.5 * attempt)

    tasks = [asyncio.create_task(run_part(i)) for i in range(parts)]
    try:
        await asyncio.gather(*tasks)
    except BaseException:
        for t in tasks:
            t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        raise


async def download_all(pool: list, queue: list, dest: Path, workers: int, parts: int):
    main_client = pool[0]
    total = sum(i.size for i in queue)
    ok, failed = [], []
    n_total = len(queue)

    q: asyncio.Queue = asyncio.Queue()
    for n, it in enumerate(queue, 1):
        q.put_nowait((n, it))

    sem = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)
    progress = Progress(
        TextColumn("[bold blue]{task.description}", justify="left"),
        BarColumn(bar_width=None), TaskProgressColumn(),
        DownloadColumn(), TransferSpeedColumn(), TimeRemainingColumn(),
        console=console,
    )

    async def worker():
        while True:
            try:
                n, it = q.get_nowait()
            except asyncio.QueueEmpty:
                return

            final = dest / it.safe_name
            tmp = dest / (it.safe_name + ".part")
            use_multi = parts > 1 and it.kind != "фото" and it.size >= MULTIPART_MIN
            label = f"({n}/{n_total}) {escape(short(it.name, 30))}" + (f" ×{parts}" if use_multi else "")
            task = progress.add_task(label, total=it.size or None)
            tr = Tracker(progress, overall, task)
            success = False

            try:
                for attempt in range(1, 4):
                    multi = use_multi and attempt < 3
                    try:
                        if multi:
                            await download_multipart(main_client, it, tmp, parts, tr, sem)
                        else:
                            res = await main_client.download_media(
                                it.msg, file=str(tmp),
                                progress_callback=lambda cur, tot, tr=tr: tr.set_abs(cur),
                            )
                            if not res:
                                raise RuntimeError("пустой результат")
                        if it.kind != "фото" and it.size and tr.done < it.size:
                            if tmp.stat().st_size < it.size:
                                raise RuntimeError("файл неполный")
                        os.replace(tmp, final)
                        success = True
                        break
                    except asyncio.CancelledError:
                        raise
                    except errors.FloodWaitError as e:
                        progress.console.print(f"[yellow]FloodWait {e.seconds}s…[/]")
                        await asyncio.sleep(e.seconds + 2)
                    except Exception as e:
                        progress.console.print(
                            f"[yellow]Ошибка «{escape(short(it.name, 30))}» ({attempt}/3): {escape(str(e))}[/]"
                        )
                        await asyncio.sleep(2)
                    tr.reset()
                    tmp.unlink(missing_ok=True)
            finally:
                if not success:
                    tmp.unlink(missing_ok=True)
                progress.remove_task(task)

            if success:
                ok.append(it)
                progress.console.print(f"[green]✓ Скачан:[/] {escape(it.name)} [dim]({human(it.size)})[/]")
            else:
                failed.append(it)
                progress.console.print(f"[red]✗ Ошибка:[/] {escape(it.name)}")

    with progress:
        overall = progress.add_task("[bold cyan]ВСЕГО", total=total or None)
        tasks = [asyncio.create_task(worker()) for _ in range(min(workers, n_total))]
        try:
            await asyncio.gather(*tasks)
        except asyncio.CancelledError:
            for t in tasks:
                t.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            raise
    return ok, failed


# ───────────────────────────── main ─────────────────────────────

async def main():
    cfg = load_config()

    main_client = await login(cfg)

    cpu = os.cpu_count() or 4
    default_pool = max(DEFAULT_POOL, min(6, cpu))
    pool_size = ask_int("Размер пула MTProto-соединений",
                        cfg.get("pool", default_pool), 1, 12)
    cfg["pool"] = pool_size
    save_config(cfg)

    pool = await build_client_pool(cfg, pool_size)

    try:
        while True:
            entity = await pick_entity(main_client)
            title = (getattr(entity, "title", None)
                     or getattr(entity, "first_name", "") or str(entity.id))
            console.print(f"[green]Канал:[/] {escape(title)}")

            photos = console.input("Включать фото? [y/N]: ").strip().lower() == "y"
            items = await scan_channel(main_client, entity, photos)
            if not items:
                console.print("[yellow]Файлов не найдено.[/]")
            else:
                selected = choose_files(items)
                if selected:
                    dest = ask_dest(cfg)
                    queue, skipped = build_queue(selected, dest)

                    for it, why in skipped:
                        console.print(f"[yellow]⏭ Пропуск ({why}):[/] {escape(it.name)}")

                    if not queue:
                        console.print("[yellow]Все выбранные файлы уже скачаны.[/]")
                    else:
                        need = sum(i.size for i in queue)
                        free = shutil.disk_usage(dest).free
                        console.print(f"\nК загрузке: [bold]{len(queue)}[/], {human(need)}. "
                                      f"Свободно: {human(free)}")
                        if need > free:
                            console.print("[red]Места может не хватить![/]")

                        workers, parts = ask_speed_settings(cfg, len(pool))
                        if console.input("Начать? [Y/n]: ").strip().lower() != "n":
                            ok, failed = await download_all(pool, queue, dest, workers, parts)
                            console.print(
                                f"\n[bold]Готово.[/] Скачано: [green]{len(ok)}[/], "
                                f"пропущено: [yellow]{len(skipped)}[/], ошибок: [red]{len(failed)}[/]"
                            )
                            console.print(f"Папка: {dest}")

            if console.input("\nДругой канал? [y/N]: ").strip().lower() != "y":
                break
    finally:
        await close_pool(pool)
        await main_client.disconnect()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        console.print("\n[yellow]Прервано. .part-файлы удалены.[/]")