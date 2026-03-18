from datetime import datetime, timezone
from typing import List, Optional

from sqlmodel import Field, Relationship, SQLModel

class Engagement(SQLModel, table=True):
    __tablename__ = "engagements"

    engagement_id: int = Field(primary_key=True)
    system_id: int = Field(index=True)
    attacker: str = Field(index=True)
    defender: str = Field(index=True)
    engagement_type: str
    start_time: datetime = Field(index=True)
    end_time: datetime = Field(index=True)
    outcome: str
    final_score: str
    active: bool = Field(default=True, index=True)
    last_checked: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


class GalaxySystem(SQLModel, table=True):
    __tablename__ = "galaxy_systems"

    system_id: int = Field(primary_key=True)
    system_name: str = Field(index=True)
    owner_name: Optional[str] = Field(default=None, index=True)
    cooldown_end: Optional[datetime] = Field(default=None, index=True)
    last_updated: datetime = Field(default_factory=lambda: datetime.now(timezone.utc), index=True)

    # Fleet wars targeting info
    is_targeted: bool = Field(default=False, index=True)
    targeting_fleet: Optional[str] = Field(default=None)
    flagged_by: Optional[int] = Field(default=None)
    admin_role_id: Optional[int] = Field(default=None)
    flagged_at: Optional[datetime] = Field(default=None)