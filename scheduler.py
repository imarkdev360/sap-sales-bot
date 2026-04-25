"""
Background Scheduler for automated alerts and maintenance tasks.

Uses APScheduler to run periodic jobs:
- Quote expiry alerts
- Approval escalation checks
- Price condition expiry monitoring
"""

import pytz
from apscheduler.schedulers.background import BackgroundScheduler
from logger_setup import get_logger

logger = get_logger(__name__)

_scheduler = None


def get_scheduler():
    """Get or create the singleton scheduler instance."""
    global _scheduler
    if _scheduler is None:
        _scheduler = BackgroundScheduler(
            job_defaults={'coalesce': True, 'max_instances': 1},
            timezone=pytz.UTC
        )
    return _scheduler


def start_scheduler(sap_handler, db_handler, sales_bot_token, manager_bot_token):
    """Initialize and start all scheduled jobs."""
    from smart_alerts import SmartAlertEngine

    scheduler = get_scheduler()
    alert_engine = SmartAlertEngine(sap_handler, db_handler, sales_bot_token, manager_bot_token)

    # Quote Expiry Check - every 6 hours
    scheduler.add_job(
        alert_engine.check_expiring_quotes,
        'interval', hours=6,
        id='quote_expiry_check',
        name='Quote Expiry Alert'
    )

    # Approval Escalation Check - every 2 hours
    scheduler.add_job(
        alert_engine.check_stale_approvals,
        'interval', hours=2,
        id='approval_escalation_check',
        name='Approval Escalation'
    )

    # Price Condition Expiry - daily at 8 AM
    scheduler.add_job(
        alert_engine.check_expiring_prices,
        'cron', hour=8,
        id='price_expiry_check',
        name='Price Expiry Monitor'
    )

    scheduler.start()
    logger.info("Background scheduler started with %d jobs.", len(scheduler.get_jobs()))
    return scheduler


def stop_scheduler():
    """Gracefully shutdown the scheduler."""
    global _scheduler
    if _scheduler and _scheduler.running:
        _scheduler.shutdown(wait=False)
        logger.info("Scheduler stopped.")
