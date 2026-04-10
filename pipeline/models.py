from pydantic import BaseModel
from typing import Optional


class Actor(BaseModel):
    id: int
    login: str


class Repo(BaseModel):
    id: int
    name: str


class Event(BaseModel):
    id: str
    type: str
    created_at: str
    actor: Actor
    repo: Repo

    # for future schema evolution (Day 3)
    device_fingerprint: Optional[str] = None