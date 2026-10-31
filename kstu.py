"""Чтение расписания группы с сайта КНИТУ (kstu.ru).

Сайт не отдаёт JSON, поэтому разбираем HTML-таблицу недели:
строки — пары (номер и время), колонки — дни, в ячейке:
<b>аудитория</b><br/>Предмет (ТИП)<br>период<br><a>Преподаватель</a><br><span>*примечание</span>
"""
import html
import re
from dataclasses import dataclass, field
from datetime import date, datetime, time

import aiohttp

BASE_URL = "https://www.kstu.ru/www_Ggrid.jsp"


@dataclass
class Lesson:
    day: date
    start: time
    end: time
    room: str
    subject: str
    kind: str          # ЛЕКЦ, ПЗ, ЛР, КСР ...
    teacher: str
    note: str          # например «1 подгр»


@dataclass
class LabSession:
    """Все пары одной лабы в один день (обе подгруппы) — одна общая очередь."""
    day: date
    subject: str
    kind: str
    start: time
    end: time
    rooms: list[str] = field(default_factory=list)
    teachers: list[str] = field(default_factory=list)
    parts: list[str] = field(default_factory=list)   # «1 подгр 09:40–14:30»

    @property
    def key(self) -> str:
        return f"kstu:{self.day.isoformat()}:{self.kind}:{self.subject}"

    def info(self) -> str:
        bits = [", ".join(self.rooms), ", ".join(self.teachers)]
        text = " · ".join(b for b in bits if b)
        if self.parts:
            text += "\n" + "; ".join(self.parts)
        return text


def _text(fragment: str) -> str:
    return " ".join(html.unescape(re.sub(r"<[^>]+>", " ", fragment)).split())


def parse_week(page: str) -> list[Lesson] | None:
    """Разбирает страницу недели. None — если таблицы расписания на странице нет."""
    start = page.find('<table class="brstu-table"')
    if start < 0:
        return None
    table = page[start:page.find("</table>", start)]
    rows = re.findall(r"<tr[^>]*>(.*?)</tr>", table, re.S)
    if not rows:
        return None

    header = re.findall(r"<td[^>]*>(.*?)</td>", rows[0], re.S)
    days = []
    for cell in header[1:]:
        m = re.search(r"(\d{2})\.(\d{2})\.(\d{4})", cell)
        days.append(date(int(m[3]), int(m[2]), int(m[1])) if m else None)
    if not any(days):
        return None

    lessons = []
    for row in rows[1:]:
        cells = re.findall(r"<td[^>]*>(.*?)</td>", row, re.S)
        if not cells:
            continue
        tm = re.search(r"(\d{1,2}):(\d{2})\s*-\s*(\d{1,2}):(\d{2})", _text(cells[0]))
        if not tm:
            continue
        t_start = time(int(tm[1]), int(tm[2]))
        t_end = time(int(tm[3]), int(tm[4]))

        for day, cell in zip(days, cells[1:]):
            if day is None:
                continue
            # в одной ячейке может быть несколько занятий — каждое начинается с <b>аудитория</b>
            for chunk in re.split(r"(?=<b>)", cell)[1:]:
                room_m = re.search(r"<b>(.*?)</b>", chunk, re.S)
                room = _text(room_m[1])
                rest = chunk[room_m.end():]  # аудитория сама может содержать скобки: «М-6(12)»
                lines = [_text(p) for p in re.split(r"<br\s*/?>", rest)]
                lines = [l for l in lines if l]
                subj_line = next((l for l in lines if re.search(r"\([^()]+\)\s*$", l)), None)
                if not subj_line:
                    continue
                sm = re.match(r"(.*)\(([^()]+)\)\s*$", subj_line)
                teacher_m = re.search(r"<a[^>]*>(.*?)</a>", chunk, re.S)
                note_m = re.search(r"<span>\s*\*?(.*?)</span>", chunk, re.S)
                lessons.append(Lesson(
                    day=day, start=t_start, end=t_end, room=room,
                    subject=sm[1].strip(), kind=sm[2].strip().upper(),
                    teacher=_text(teacher_m[1]) if teacher_m else "",
                    note=" ".join(_text(note_m[1]).split()) if note_m else "",
                ))
    return lessons


def group_sessions(lessons: list[Lesson], kinds: set[str]) -> list[LabSession]:
    """Склеивает пары одной лабы за день в одну сессию (общую для подгрупп)."""
    sessions: dict[tuple, LabSession] = {}
    by_note: dict[tuple, list[Lesson]] = {}
    for l in sorted(lessons, key=lambda x: (x.day, x.start)):
        if l.kind not in kinds:
            continue
        k = (l.day, l.kind, l.subject)
        s = sessions.get(k)
        if s is None:
            s = sessions[k] = LabSession(l.day, l.subject, l.kind, l.start, l.end)
        s.start, s.end = min(s.start, l.start), max(s.end, l.end)
        if l.room and l.room not in s.rooms:
            s.rooms.append(l.room)
        if l.teacher and l.teacher not in s.teachers:
            s.teachers.append(l.teacher)
        by_note.setdefault(k + (l.note,), []).append(l)

    for (day, kind, subject, note), ls in by_note.items():
        if note and "подгр" in note:
            sessions[(day, kind, subject)].parts.append(
                f"{note}: {ls[0].start:%H:%M}–{ls[-1].end:%H:%M}")
    for s in sessions.values():
        s.parts.sort()
    return sorted(sessions.values(), key=lambda s: (s.day, s.start))


async def fetch_week(session: aiohttp.ClientSession, faculty: str, group: str,
                     day: date) -> list[Lesson] | None:
    params = {"d": day.isoformat(), "f": faculty, "g": group}
    async with session.get(BASE_URL, params=params,
                           timeout=aiohttp.ClientTimeout(total=30)) as resp:
        resp.raise_for_status()
        page = await resp.text(errors="replace")
    return parse_week(page)


def combine(day: date, t: time, tz) -> datetime:
    return datetime.combine(day, t, tz)
