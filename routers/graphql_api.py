from __future__ import annotations

import asyncio

import strawberry
from strawberry.fastapi import GraphQLRouter

from routers import redis_state


@strawberry.type
class Task:
    id: str
    title: str | None
    owner: str | None
    deadline: str | None
    status: str | None


@strawberry.type
class Decision:
    id: str
    statement: str | None
    speaker: str | None


def _fetch_hashes(pattern: str) -> list[dict[str, str]]:
    r = redis_state.get_redis()
    rows: list[dict[str, str]] = []
    for key in r.scan_iter(match=pattern):
        h = r.hgetall(key)
        if h:
            rows.append(h)
    return rows


@strawberry.type
class Query:
    @strawberry.field
    async def tasks(self) -> list[Task]:
        rows = await asyncio.to_thread(_fetch_hashes, "task:*")
        out: list[Task] = []
        for h in rows:
            task_id = (h.get("id") or "").strip()
            if not task_id:
                continue
            out.append(
                Task(
                    id=task_id,
                    title=h.get("title"),
                    owner=h.get("owner"),
                    deadline=h.get("deadline"),
                    status=h.get("status"),
                )
            )
        return out

    @strawberry.field
    async def decisions(self) -> list[Decision]:
        rows = await asyncio.to_thread(_fetch_hashes, "decision:*")
        out: list[Decision] = []
        for h in rows:
            decision_id = (h.get("id") or "").strip()
            if not decision_id:
                continue
            out.append(
                Decision(
                    id=decision_id,
                    statement=h.get("statement"),
                    speaker=h.get("speaker"),
                )
            )
        return out


schema = strawberry.Schema(query=Query)
router = GraphQLRouter(schema)

