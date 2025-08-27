from datetime import datetime
import uuid
from typing import List, Optional, Dict
from redis_client import RedisClientSingleton
from utils import get_current_timestamp, get_parsed_timestamp

class RedisTaskStore:
    def __init__(self):
        # Assumes the underlying client was created with decode_responses=True
        self.redis = RedisClientSingleton.get_client()

    def _task_key(self, task_id: str) -> str:
        return f"task:{task_id}"

    def _status_key(self, user_id: str, status: str) -> str:
        return f"tasks:{user_id}:status:{status}"

    def _due_key(self, user_id: str) -> str:
        return f"tasks:{user_id}:due"

    # ---------------------------
    # CRUD Operations
    # ---------------------------

    def create_task(
        self,
        user_id: str,
        description: str,
        status: str = "pending",
        due_time: Optional[int] = None,
    ) -> str:
        """
        Create a task and index it by status and (optionally) due_time.
        due_time: epoch seconds (int) or None
        """
        task_id = str(uuid.uuid4())
        task_key = self._task_key(task_id)

        task_data = {
            "id": task_id,
            "user_id": user_id,
            "description": description,
            "status": status,
            "due_time": str(int(due_time)) if due_time is not None else "",
            "created_at": str(get_current_timestamp()),
        }

        # Store main record
        self.redis.hset(task_key, mapping=task_data)

        # Index by status
        self.redis.sadd(self._status_key(user_id, status), task_id)

        # Index by due time (sorted set score = due_time)
        if due_time is not None:
            self.redis.zadd(self._due_key(user_id), {task_id: int(due_time)})

        return task_id

    def create_tasks_bulk(self, user_id: str, tasks: List[Dict]) -> List[str]:
        """
        Bulk create multiple tasks using redis.pipeline() for speed.
        Each task dict should have:
            {
                "description": str,
                "status": str (optional, default "pending"),
                "due_time": int (optional, epoch seconds)
            }
        Returns: list of created task_ids.
        """
        task_ids: List[str] = []
        pipe = self.redis.pipeline(transaction=True)
        now = int(datetime.now().timestamp())

        for task in tasks:
            task_id = str(uuid.uuid4())
            task_ids.append(task_id)

            description = task["description"]
            status = task.get("status", "pending")
            due_time = int(task["due_time"]) if task.get("due_time") is not None else None

            task_key = self._task_key(task_id)
            task_data = {
                "id": task_id,
                "user_id": user_id,
                "description": description,
                "status": status,
                "due_time": str(due_time) if due_time is not None else "",
                "created_at": str(now),
            }

            # Batch operations
            pipe.hset(task_key, mapping=task_data)
            pipe.sadd(self._status_key(user_id, status), task_id)
            if due_time is not None:
                pipe.zadd(self._due_key(user_id), {task_id: due_time})

        pipe.execute()
        return task_ids

    def get_task(self, task_id: str) -> Optional[Dict]:
        task_key = self._task_key(task_id)
        task = self.redis.hgetall(task_key)
        if task:
            task["due_time"] = datetime.fromtimestamp(int(task["due_time"])).strftime("%Y-%m-%d %H:%M:%S") if task.get("due_time") not in (None, "", "0") else None
            
        return dict(task) if task else None

    def update_task(self, user_id: str, task_id: str, updates: Dict) -> bool:
        """
        Update a task and reindex if status or due_time changed.
        updates: dict of fields to update; values will be stored as strings in the hash
        """
        task_key = self._task_key(task_id)
        task = self.redis.hgetall(task_key)
        if not task:
            return False

        old_status = task["status"]
        old_due_time = int(task.get("due_time")) if task.get("due_time") not in (None, "", "0") else None
        new_due_time = None
        
        if "due_time" in updates and updates.get("due_time") not in (None, "", "None"):
            new_due_time = get_parsed_timestamp(updates.get("due_time"))
        else:
            new_due_time = old_due_time

        # print(f"Old time: {old_due_time}   New time : {new_due_time}")
        # Apply updates to in-memory dict (store as strings)
        for k, v in updates.items():
            if k == "due_time":
                # print(f"k: {k}, v: {v}")
                # print(f"new_due_time: {new_due_time}")
                task[k] = str(int(new_due_time)) if new_due_time is not None else ""
            else:
                task[k] = "" if v is None else str(v)

        # Persist hash
        self.redis.hset(self._task_key(task_id), mapping=task)

        # Reindex by status if needed
        if "status" in updates:
            new_status = updates["status"]
            if new_status != old_status:
                self.redis.srem(self._status_key(user_id, old_status), task_id)
                self.redis.sadd(self._status_key(user_id, new_status), task_id)

        # Reindex by due time if needed
        if "due_time" in updates:
            # Remove old score if it existed
            if old_due_time:
                self.redis.zrem(self._due_key(user_id), task_id)

            # Add new score if provided
            if new_due_time is not None:
                self.redis.zadd(self._due_key(user_id), {task_id: int(new_due_time)})

        return True

    def delete_task(self, user_id: str, task_id: str) -> bool:
        task = self.get_task(task_id)
        
        if not task:
            return False

        due_time = task.get("due_time")  # string or ""

    
        if task.get("status") not in (None, "", "0"):
            status = task["status"] 
            # Remove from indexes
            self.redis.srem(self._status_key(user_id, status), task_id)
            
        if due_time:
            self.redis.zrem(self._due_key(user_id), task_id)

        # Delete main record
        self.redis.delete(self._task_key(task_id))
        return True

    # ---------------------------
    # Query Operations
    # ---------------------------

    def get_tasks_by_status(self, user_id: str, status: str) -> List[Dict]:
        task_ids = self.redis.smembers(self._status_key(user_id, status))
        return [self.get_task(tid) for tid in task_ids]

    def get_tasks_by_due_range(self, user_id: str, start: int, end: int) -> List[Dict]:
        """
        Returns tasks with due_time in [start, end].
        NOTE: start/end are inclusive. If you pass start == end,
        only tasks exactly at that second will match.
        """
        task_ids = self.redis.zrangebyscore(self._due_key(user_id), start, end)
        return [self.get_task(tid) for tid in task_ids]

    def get_tasks_by_filters(
        self,
        user_id: str,
        statuses: Optional[List[str]] = None,
        start: Optional[int] = None,
        end: Optional[int] = None,
    ) -> List[Dict]:
        """
        Flexible filter:
          - If statuses is provided, filter by those statuses.
          - If start/end provided, filter by due_time in [start, end].
          - If both provided, return intersection.
          - If only one filter is provided, return matches for that filter.
          - If neither is provided, returns [] (explicit by design).

        Tips:
          - Omit start or end to make open-ended ranges.
            e.g., start=None, end=some_ts  -> (-inf, some_ts]
                  start=some_ts, end=None  -> [some_ts, +inf)
        """
        # Build the time filter first using open bounds if missing
        min_arg = start if start is not None and len(str(start)) > 0 else "-inf"
        max_arg = end if end is not None and len(str(end)) > 0 else "+inf"

        time_ids: Optional[set] = None
        # Only query time index if a bound is provided (to avoid returning only tasks with due_time
        # when caller didn't ask for a time filter).
        if start is not None or end is not None:
            time_ids = set(self.redis.zrangebyscore(self._due_key(user_id), min_arg, max_arg))

        status_ids: Optional[set] = None
        if statuses:
            # Union across all requested statuses
            buckets = [self.redis.smembers(self._status_key(user_id, s)) for s in statuses]
            status_ids = set().union(*buckets) if buckets else set()

        # Decide what to return based on which filters are present
        if status_ids is not None and time_ids is not None:
            ids = status_ids & time_ids
        elif status_ids is not None:
            ids = status_ids
        elif time_ids is not None:
            ids = time_ids
        else:
            # No filters -> explicit empty (change to "all tasks" if desired)
            return []

        if not ids:
            return []

        return [self.get_task(tid) for tid in ids]
