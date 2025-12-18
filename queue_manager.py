import redis
import json
import time
import os

class QueueManager:
    def __init__(self, host=None, port=6379, db=0):
        self.host = host or os.getenv("REDIS_HOST", "localhost")
        self.port = port
        self.db = db
        self.r = redis.Redis(host=self.host, port=self.port, db=self.db, decode_responses=True)

        self.pending_queue = "tasks:pending"
        self.processing_queue = "tasks:processing"
        # We store processing metadata in a hash to track timestamps for the Janitor
        self.processing_meta = "tasks:processing:meta"

        # Lua script for atomic pop and timestamp set
        self._pop_script = self.r.register_script("""
            local task = redis.call('RPOPLPUSH', KEYS[1], KEYS[2])
            if task then
                redis.call('HSET', KEYS[3], task, ARGV[1])
            end
            return task
        """)

    def push_task(self, task):
        """Pushes a task to the pending queue."""
        self.r.lpush(self.pending_queue, json.dumps(task))

    def pop_task(self, worker_id):
        """
        Reliably pops a task using atomic Lua script (RPOPLPUSH + HSET).
        Moves task from pending to processing and records start time.
        """
        current_time = time.time()

        # We use the Lua script to ensure atomicity
        # Keys: pending, processing, meta
        # Arg: current_time

        task_json = self._pop_script(
            keys=[self.pending_queue, self.processing_queue, self.processing_meta],
            args=[current_time]
        )

        if task_json:
            return json.loads(task_json)
        return None

    def complete_task(self, task):
        """
        Removes the task from the processing queue and metadata, signifying completion.
        """
        task_json = json.dumps(task)
        pipeline = self.r.pipeline()
        pipeline.lrem(self.processing_queue, 0, task_json)
        pipeline.hdel(self.processing_meta, task_json)
        pipeline.execute()

    def janitor(self, timeout_seconds=600):
        """
        Recover tasks that have been in processing for too long (> timeout_seconds).
        Move them back to pending.
        """
        processing_tasks = self.r.hgetall(self.processing_meta)
        current_time = time.time()

        recovered_count = 0

        for task_json, start_time in processing_tasks.items():
            if current_time - float(start_time) > timeout_seconds:
                # Task is stale.
                # Move back to pending (LPUSH) and remove from processing (LREM) and meta (HDEL).

                pipeline = self.r.pipeline()
                pipeline.lrem(self.processing_queue, 0, task_json) # Remove from processing
                pipeline.hdel(self.processing_meta, task_json) # Remove meta
                pipeline.lpush(self.pending_queue, task_json) # Re-queue
                results = pipeline.execute()

                if results[0] > 0:
                    print(f"Janitor: Recovered stale task: {task_json[:50]}...")
                    recovered_count += 1

        return recovered_count

    def clear_queues(self):
        self.r.delete(self.pending_queue)
        self.r.delete(self.processing_queue)
        self.r.delete(self.processing_meta)
