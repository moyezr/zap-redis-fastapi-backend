from fastapi import FastAPI, Depends, HTTPException, status
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field, validator
from typing import List, Optional
from datetime import datetime
import uuid
from contextlib import asynccontextmanager
from task_store import RedisTaskStore
from utils import get_parsed_timestamp


# Pydantic models
class TaskBase(BaseModel):
    description: str = Field(..., min_length=1, max_length=500)
    due_time: Optional[str] = None
    status: Optional[str] = "pending"

    @validator("status")
    def validate_status(cls, v):
        if v not in ["pending", "completed"]:
            raise ValueError('Status must be either "pending" or "completed"')
        return v


class TaskCreate(TaskBase):
    user_id: str


class TaskUpdate(BaseModel):
    description: Optional[str] = Field(None, min_length=1, max_length=500)
    due_time: Optional[str] = None
    status: Optional[str] = None
    user_id: str

    @validator("status")
    def validate_status(cls, v):
        if v and v not in ["pending", "completed"]:
            raise ValueError('Status must be either "pending" or "completed"')
        return v


class TaskResponse(TaskBase):
    id: str
    user_id: str
    created_at: datetime

    class Config:
        orm_mode = True


class TasksRequest(BaseModel):
    user_id: str
    tasks: List[TaskCreate]


# Application lifespan management
@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    app.state.task_store = RedisTaskStore()
    yield
    # Shutdown
    # Cleanup if needed


# Create FastAPI app
app = FastAPI(
    title="Task Management API",
    description="API for managing user tasks",
    version="1.0.0",
    lifespan=lifespan,
)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Adjust in production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# Dependency
def get_task_store():
    return app.state.task_store


# Routes
@app.get("/health")
async def health_check():
    return {"status": "healthy", "timestamp": datetime.now()}


@app.get("/tasks", response_model=List[TaskResponse])
async def get_tasks(
    user_id: str,
    statuses: Optional[List[str]] = None,
    start_time: Optional[str] = None,
    end_time: Optional[str] = None,
    task_store: RedisTaskStore = Depends(get_task_store),
):
    try:
        # Convert string timestamps to integers if provided
        start_ts = get_parsed_timestamp(start_time) if start_time else None
        end_ts = get_parsed_timestamp(end_time) if end_time else None

        print(f"USER: {user_id}")
        print(f"statuses: {statuses}")

        if not statuses:
            statuses = ["pending", "completed"]

        tasks = task_store.get_tasks_by_filters(
            user_id,
            statuses=statuses,
            start=start_ts,
            end=end_ts,  # Hardcoded for now
        )
        
        tasks = [task for task in tasks if task is not None]
        return tasks
    except Exception as e:
        print(f"ERROR FETCHIONG TASK {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error retrieving tasks: {str(e)}",
        )

@app.get("/task/{task_id}", response_model=TaskResponse)
async def get_task(
    task_id: str,
    task_store: RedisTaskStore = Depends(get_task_store),
):
    try:
        # Convert string timestamps to integers if provided
       
        tasks = task_store.get_task(task_id=task_id)
        return tasks
    except Exception as e:
        print(f"ERROR FETCHIONG TASK {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error retrieving tasks: {str(e)}",
        )


@app.post("/tasks", response_model=List[str], status_code=status.HTTP_201_CREATED)
async def create_tasks(
    request: TasksRequest, task_store: RedisTaskStore = Depends(get_task_store)
):
    try:

        tasks = request.tasks
        user_id = request.user_id
        print(f"Received tasks: {tasks}")
        task_dicts = [
            {
                "description": task.description,
                "user_id": task.user_id,
                "status": task.status,
                "due_time": (
                    get_parsed_timestamp(task.due_time) if task.due_time else None
                ),
            }
            for task in tasks
        ]

        task_ids = task_store.create_tasks_bulk(user_id, task_dicts)
        return task_ids
    except Exception as e:
        print(f"Error: {str(e)}")  # Add this debug line
        print(f"Tasks: {tasks}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error creating tasks: {str(e)}",
        )


@app.put("/tasks/{task_id}")
async def update_task(
    task_id: str,
    updates: TaskUpdate,
    task_store: RedisTaskStore = Depends(get_task_store),
):
    try:
        update_dict = updates.model_dump(exclude_unset=True)
        
        print(f"updates: {update_dict}")
        print(f"task_id: {task_id}")



        user_id = update_dict.pop("user_id", None)  # Default to "user-1" if not provided
        
        if not user_id:
            raise Exception("user_id is required for updating a task")
        success = task_store.update_task(user_id, task_id, update_dict)
        
        if not success:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND, detail="Task not found"
            )
        return {"message": "Task updated successfully"}
    except HTTPException:
        raise
    except Exception as e:
        print(f"Error: {str(e)}")  # Add this debug line
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error updating task: {str(e)}",
        )


@app.delete("/tasks/{task_id}")
async def delete_task(
    task_id: str, task_store: RedisTaskStore = Depends(get_task_store)
):
    try:
        success = task_store.delete_task("user-1", task_id)
        if not success:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND, detail="Task not found"
            )
        return {"message": "Task deleted successfully"}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error deleting task: {str(e)}",
        )


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
