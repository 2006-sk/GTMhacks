from __future__ import annotations

import asyncio

_history: list[dict[str, str]] = []
_lock = asyncio.Lock()


async def append_chunk(speaker: str, text: str) -> int:
    entry = {"speaker": speaker, "text": text}
    async with _lock:
        _history.append(entry)
        return len(_history)


async def pop_if_last(speaker: str, text: str) -> None:
    entry = {"speaker": speaker, "text": text}
    async with _lock:
        if _history and _history[-1] == entry:
            _history.pop()


async def history_length() -> int:
    async with _lock:
        return len(_history)


async def history_snapshot() -> list[dict[str, str]]:
    async with _lock:
        return list(_history)

