from dotenv import load_dotenv

from app.utils import get_ip_info

load_dotenv()

import random
import anyio
from contextlib import asynccontextmanager
from functools import partial
import logging

# Import third-party libraries
import uvicorn
from fastapi import FastAPI, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.responses import JSONResponse
from firebase_admin import auth

# Configure basic logging before importing application modules
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler()],
    force=True,
)

logger = logging.getLogger(__name__)

# Now import application modules AFTER environment variables are loaded
from app.tasks.shadows_suggestions import shadows_suggestions_task

from app.api import (
    account_suggestion_api,
    app_config_api,
    healthcheck_api,
    posts_api,
    powerbi_api,
    purchase_tracking_api,
    roles_api,
    shadows_debug_api,
    users_api,
    cart_manager_api,
    log_user_navigation_api,
    virtual_order_api,
    emails_api,
    ams_api,
    open_distribution_api,
    ams_api_key_based_get_api,
    reports_api,
    report_category_api,
    super_priority_events_api,
    po_queue_api,
    tm_queue_tracking_api,
    user_favourite_pages_api,
    email_filter_api,
    unclaimed_sales_api,
    subs_report_api,
    buylist_api,
    shadows_blacklist_api,
    shadows_wildcard_blacklist_api,
    shadows_listings_api,
    shadows_stats_api,
    shadows_viagogo_event_mapping,
    shadows_vivid_event_mapping,
    shadows_offer_types_api,
    shadows_30day_mapping_api,
    onsale_email_api,
    onsale_email_analysis_api,
    onsale_chat_api,
    cart_manager_filter_api,
    shadows_listing_stats_api,
    shadows_pricing_report_api,
    shadows_tessitura_whitelist_api,
    shadows_ticketmaster_api,
    a_to_z_report_api,
    csv_listing_sync_api,
    incoming_texts_api,
    shadows_autopricing_config_api,
    seatgeek_listings_api,
    snowflake_logs_api,
    shadows_config_api,
    price_change_monitor_api,
)
from app.database import close_pg_database, init_pg_database
from app.service import firebase_auth_factory

auth_excluded_routes = {
    "/healthcheck": "GET",
    "/docs": "GET",
    "/openapi.json": "GET",
    "/users/bootstrap": "GET",
}

root_logger = logging.getLogger()
root_logger.setLevel(logging.INFO)

# Configure uvicorn access logger
logging.getLogger("uvicorn.access").setLevel(logging.INFO)
logging.getLogger("uvicorn.error").setLevel(logging.INFO)

# Set specific logger levels for our application
app_logger = logging.getLogger("app")
app_logger.setLevel(logging.INFO)
app_logger.propagate = True

api_logger = logging.getLogger("app.api")
api_logger.setLevel(logging.INFO)
api_logger.propagate = True

logger = logging.getLogger(__name__)
logger.info("=== Application starting up - Logging configured ===")

# Initialize Firebase
firebase_auth_factory.initialize_firebase()


async def supervise(stop: anyio.Event, worker_coro, *args, worker_name: str, base_delay: float = 1.0, max_delay: float = 30.0):
    attempt = 0
    while not stop.is_set():
        try:
            logger.info(f"Starting supervised task: {worker_name}")
            await worker_coro(stop, *args)
            attempt = 0
            break
        except anyio.get_cancelled_exc_class():
            logger.info(f"Supervised task {worker_name} received cancellation signal")
            raise
        except Exception as e:
            attempt += 1
            delay = min(max_delay, base_delay * (2 ** (attempt - 1)))
            delay = delay * (0.8 + random.random() * 0.4)  # jitter
            logger.error("[%s] crashed (attempt %d). Restarting in %.1fs", worker_name, attempt, delay, exc_info=True)

            if stop.is_set():
                logger.info(f"Stop signal detected for {worker_name}, not restarting")
                break

            try:
                await anyio.sleep(delay)
            except anyio.get_cancelled_exc_class():
                logger.info(f"Supervised task {worker_name} cancelled during restart delay")
                raise

    logger.info(f"Supervised task {worker_name} exiting cleanly")


@asynccontextmanager
async def lifespan(app: FastAPI):
    stop = anyio.Event()
    logger.info("Starting application initialization...")

    # --- Startup code ---
    try:
        ip_info = await get_ip_info()
        logger.info("IP Information: %s", ip_info)
        await init_pg_database()
        logger.info("Database connections initialized successfully")
    except Exception as e:
        logger.error("Failed to initialize database connections: %s", str(e), exc_info=True)
        raise

    async with anyio.create_task_group() as tg:
        tg.start_soon(
            partial(supervise, worker_name="shadows_suggester"),
            stop,
            shadows_suggestions_task,
        )
        try:
            # --- Application is running ---
            yield
        finally:
            # --- Shutdown code ---
            logger.info("Shutting down application...")
            stop.set()
            logger.info("Cancelling background tasks...")
            tg.cancel_scope.cancel()

    logger.info("Closing database connections...")
    await close_pg_database()
    logger.info("Database connections closed successfully")
    logger.info("Application shutdown complete.")


app = FastAPI(lifespan=lifespan)

# Include your routers
app.include_router(healthcheck_api.router)
app.include_router(roles_api.router)
app.include_router(users_api.router)
app.include_router(posts_api.router)
app.include_router(purchase_tracking_api.router)
app.include_router(powerbi_api.router)
app.include_router(emails_api.router)
app.include_router(ams_api.router)
app.include_router(account_suggestion_api.router)
app.include_router(open_distribution_api.router)
app.include_router(ams_api_key_based_get_api.router)
app.include_router(cart_manager_api.router)
app.include_router(cart_manager_filter_api.router)
app.include_router(log_user_navigation_api.router)
app.include_router(virtual_order_api.router)
app.include_router(reports_api.router)
app.include_router(report_category_api.router)
app.include_router(super_priority_events_api.router)
app.include_router(po_queue_api.router)
app.include_router(tm_queue_tracking_api.router)
app.include_router(user_favourite_pages_api.router)
app.include_router(email_filter_api.router)
app.include_router(unclaimed_sales_api.router)
app.include_router(subs_report_api.router)
app.include_router(buylist_api.router)
app.include_router(shadows_blacklist_api.router)
app.include_router(shadows_wildcard_blacklist_api.router)
app.include_router(shadows_listings_api.router)
app.include_router(shadows_stats_api.router)
app.include_router(shadows_viagogo_event_mapping.router)
app.include_router(shadows_vivid_event_mapping.router)
app.include_router(shadows_offer_types_api.router)
app.include_router(shadows_30day_mapping_api.router)
app.include_router(onsale_email_api.router)
app.include_router(onsale_email_analysis_api.router)
app.include_router(onsale_chat_api.router)
app.include_router(shadows_listing_stats_api.router)
app.include_router(shadows_pricing_report_api.router)
app.include_router(shadows_tessitura_whitelist_api.router)
app.include_router(shadows_ticketmaster_api.router)
app.include_router(a_to_z_report_api.router)
app.include_router(csv_listing_sync_api.router)
app.include_router(incoming_texts_api.router)
app.include_router(shadows_autopricing_config_api.router)
app.include_router(shadows_debug_api.router)
app.include_router(seatgeek_listings_api.router)
app.include_router(snowflake_logs_api.router)
app.include_router(app_config_api.router)
app.include_router(shadows_config_api.router)
app.include_router(price_change_monitor_api.router)

# Enable CORS for all domains
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Allows all origins
    allow_credentials=True,
    allow_methods=["*"],  # Allows all methods
    allow_headers=["*"],  # Allows all headers
)

# Enable gzip compression middleware
app.add_middleware(GZipMiddleware, minimum_size=500)


@app.middleware("http")
async def check_authorization_header(request: Request, call_next):
    # If it's an OPTIONS request, we bypass the authorization check
    if request.method == "OPTIONS":
        return await call_next(request)

    if (auth_excluded_routes.get(request.url.path, "").lower() == request.method.lower()
            # exclude all ams-local routes from auth check
            or request.url.path.startswith("/ams-local")):
        return await call_next(request)

    try:
        headers = request.headers
        token = headers.get("Authorization")
        if not token:
            return Response(content="Missing Auth Token", status_code=500)

        auth.verify_id_token(token.split("Bearer ")[1])

        return await call_next(request)
    except auth.ExpiredIdTokenError as e:
        return JSONResponse(content={"error": "Token expired"}, status_code=401)
    except Exception as e:
        logging.error(e.args)
        return Response(content=str(e), status_code=500)


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8080)
