from datetime import datetime
from pydantic import BaseModel, Field
class DrillInput(BaseModel):
    scenario_id: int = Field(gt=0)
    label: str = Field(min_length=2, max_length=120)
    ttl_seconds: int | None = Field(default=None, gt=0)
    deadline_at: datetime | None = None
