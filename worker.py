import asyncio
import os
import json
import logging
import aiohttp
import aiofiles
import datetime
from sqlalchemy.future import select
from sqlalchemy.dialects.postgresql import insert
from models import AsyncSessionLocal, Business
from queue_manager import QueueManager
from geo_utils import calculate_circumscribed_radius, calculate_zoom_level, split_square

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] Worker: %(message)s')
logger = logging.getLogger(__name__)

# DataForSEO Credentials
# To use hardcoded credentials, replace "REPLACE_WITH_YOUR_LOGIN" and "REPLACE_WITH_YOUR_PASSWORD"
# with your actual login and password strings.
API_USERNAME = os.getenv("DATAFORSEO_LOGIN", "REPLACE_WITH_YOUR_LOGIN")
API_PASSWORD = os.getenv("DATAFORSEO_PASSWORD", "REPLACE_WITH_YOUR_PASSWORD")
API_BASE = "https://api.dataforseo.com/v3"

queue = QueueManager()

async def post_task(session, task_data):
    """
    Posts a task to DataForSEO Google Maps Local Pack Task endpoint.
    """
    url = f"{API_BASE}/google/maps/task_post"

    radius_meters = calculate_circumscribed_radius(task_data['width'])
    zoom = calculate_zoom_level(radius_meters, task_data['lat'])

    # Payload
    post_data = []
    post_data.append({
        "language_code": "en",
        "location_coordinate": f"{task_data['lat']},{task_data['lng']},{zoom}",
        "keyword": task_data['keyword'],
        "priority": 1 # Standard priority
    })

    auth = aiohttp.BasicAuth(API_USERNAME, API_PASSWORD)

    try:
        async with session.post(url, json=post_data, auth=auth) as response:
            result = await response.json()
            if result.get('status_code') == 20000:
                task_id = result['tasks'][0]['id']

                # Log task_id locally
                try:
                    os.makedirs("data", exist_ok=True)
                    log_entry = {
                        "task_id": task_id,
                        "timestamp": datetime.datetime.utcnow().isoformat(),
                        "keyword": task_data['keyword'],
                        "lat": task_data['lat'],
                        "lng": task_data['lng'],
                        "width": task_data['width'],
                        "zoom": zoom
                    }
                    async with aiofiles.open("data/task_ids.jsonl", mode='a') as f:
                        await f.write(json.dumps(log_entry) + "\n")
                except Exception as log_err:
                    logger.error(f"Failed to log task_id locally: {log_err}")

                return task_id
            else:
                logger.error(f"Error posting task: {result}")
                return None
    except Exception as e:
        logger.error(f"Exception posting task: {e}")
        return None

async def get_task_result(session, task_id):
    """
    Polls for the result of a task.
    """
    url = f"{API_BASE}/google/maps/task_get/regular/{task_id}"
    auth = aiohttp.BasicAuth(API_USERNAME, API_PASSWORD)

    # Poll loop
    max_retries = 20
    for _ in range(max_retries):
        try:
            async with session.get(url, auth=auth) as response:
                result = await response.json()
                if result.get('status_code') == 20000:
                    if result['tasks'][0]['status_code'] == 20000:
                        return result['tasks'][0]['result']
                    elif result['tasks'][0]['status_code'] == 40602:
                        # Task in progress
                        await asyncio.sleep(5)
                        continue
                    else:
                        logger.error(f"Task failed with status: {result['tasks'][0]['status_code']}")
                        return None
                else:
                     logger.error(f"Error getting task: {result}")
                     return None
        except Exception as e:
            logger.error(f"Exception getting task result: {e}")
            await asyncio.sleep(5)

        await asyncio.sleep(2)
    return None

async def save_business(business_data, keyword):
    """
    Saves or updates a business in the database using ON CONFLICT logic.
    """
    async with AsyncSessionLocal() as session:
        async with session.begin():
            # Prepare insert statement
            insert_stmt = insert(Business).values(
                place_id=business_data['place_id'],
                name=business_data.get('title'),
                city=business_data.get('address_info', {}).get('city'),
                full_address=business_data.get('address'),
                category=business_data.get('category'),
                website=business_data.get('url'),
                maps_url=business_data.get('url'),
                keywords_found=[keyword]
            )

            do_update_stmt = insert_stmt.on_conflict_do_update(
                index_elements=['place_id'],
                set_={
                    'keywords_found': Business.keywords_found + insert_stmt.excluded.keywords_found,
                    'name': insert_stmt.excluded.name,
                    'city': insert_stmt.excluded.city,
                    'full_address': insert_stmt.excluded.full_address,
                    'category': insert_stmt.excluded.category,
                    'website': insert_stmt.excluded.website,
                    'maps_url': insert_stmt.excluded.maps_url
                }
            )

            await session.execute(do_update_stmt)

        await session.commit()

async def process_task(task, semaphore):
    """
    Orchestrates the processing of a single task, limited by a semaphore.
    """
    async with semaphore:
        logger.info(f"Processing task: {task}")

        if not API_USERNAME or not API_PASSWORD:
            logger.error("API credentials missing.")
            # We don't complete the task so it can be retried if this is transient,
            # but if it's permanent config error, we are stuck.
            # Assuming env vars are present.
            pass

        async with aiohttp.ClientSession() as session:
            # 1. Post Task
            task_id = await post_task(session, task)
            if not task_id:
                queue.complete_task(task)
                return

            # 2. Poll Result
            results_list = await get_task_result(session, task_id)
            if results_list is None:
                 queue.complete_task(task)
                 return

            items = []
            for res_obj in results_list:
                 if 'items' in res_obj:
                     items.extend(res_obj['items'])

            count = len(items)
            logger.info(f"Task {task_id} returned {count} items.")

            # 3. Logic:
            if count >= 100:
                # Split
                logger.info("Splitting square...")
                sub_squares = split_square(task['lat'], task['lng'], task['width'])
                for (new_lat, new_lng, new_width) in sub_squares:
                    new_task = {
                        "lat": new_lat,
                        "lng": new_lng,
                        "width": new_width,
                        "keyword": task['keyword']
                    }
                    queue.push_task(new_task)
            elif count > 0:
                # Save
                logger.info(f"Saving {count} businesses...")
                # Save concurrently? Usually DB IO is fast enough, but we can gather.
                save_tasks = [save_business(item, task['keyword']) for item in items]
                await asyncio.gather(*save_tasks)

            # Finally, mark original task as complete
            queue.complete_task(task)


async def worker_loop(worker_id):
    logger.info(f"Worker {worker_id} started.")

    # Intra-worker concurrency limit
    MAX_CONCURRENT_TASKS = 10
    semaphore = asyncio.Semaphore(MAX_CONCURRENT_TASKS)

    # We need to keep track of running tasks to not exit prematurely if we wanted graceful shutdown,
    # but here we just loop.

    active_tasks = set()

    while True:
        # Clean up finished tasks from our tracking set
        active_tasks = {t for t in active_tasks if not t.done()}

        # If we have capacity, try to pop a task
        if len(active_tasks) < MAX_CONCURRENT_TASKS:
            try:
                task = queue.pop_task(worker_id)
                if task:
                    # Create async task
                    t = asyncio.create_task(process_task(task, semaphore))
                    active_tasks.add(t)
                    # Don't sleep if we got a task, try to fill capacity
                    continue
                else:
                    # Queue empty
                    await asyncio.sleep(1)
            except Exception as e:
                logger.error(f"Worker loop error: {e}")
                await asyncio.sleep(5)
        else:
            # Capacity full, wait a bit
            await asyncio.sleep(0.1)

        # Janitor check (unchanged)
        import random
        if random.random() < 0.01:
             try:
                 queue.janitor()
             except Exception as e:
                 logger.error(f"Janitor error: {e}")

if __name__ == "__main__":
    w_id = os.getenv("HOSTNAME", "worker-0")
    asyncio.run(worker_loop(w_id))
