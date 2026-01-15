from pydantic import BaseModel


class TaskStatusUpdateRequest(BaseModel):
    is_task_complete: bool


class StarStatusUpdateRequest(BaseModel):
    is_starred: bool
