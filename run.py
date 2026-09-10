import argparse
import logging
import signal
import shutil
import subprocess
import sys
import time
from pathlib import Path

# ============================================================
# CONFIGURATION
# ============================================================

BASE_DIR = Path(__file__).resolve().parent

PROCESSOR_SCRIPT = BASE_DIR / "judgment_processor.py"
SCRAPER_SCRIPT = BASE_DIR / "download.py"

ORDERS_DIR = BASE_DIR / "data" / "court" / "cnrorders" / "cmis" / "orders"

# Temporary directory containing solved captcha files.
CAPTCHA_TMP_DIR = BASE_DIR / "captcha-tmp"

# How often we check whether the queue is empty
QUEUE_CHECK_INTERVAL_SECONDS = 5

# How many consecutive empty checks we require before considering
# the queue drained. This protects against a scraper still writing
# files while the directory happens to be temporarily empty.
QUEUE_EMPTY_CONFIRMATIONS = 3


# ============================================================
# LOGGING
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

logger = logging.getLogger("nyaysaathi-runner")


# ============================================================
# GLOBAL PROCESS REFERENCES
# ============================================================

processor_process = None
scraper_process = None


# ============================================================
# QUEUE
# ============================================================


def get_queue_files():
    """
    Return all PDF/JSON files currently present in orders/.

    We deliberately look at both extensions because a judgment
    is represented by a PDF + matching JSON pair.
    """

    if not ORDERS_DIR.exists():
        return []

    return list(ORDERS_DIR.glob("*.pdf")) + list(ORDERS_DIR.glob("*.json"))


def queue_is_empty():
    """
    True when there are no PDF or JSON files remaining in orders/.
    """

    return len(get_queue_files()) == 0


def wait_for_queue_to_drain():
    """
    Wait until judgment_processor.py has drained the queue.

    IMPORTANT:
    The scraper has already completely exited before this function is called.
    Therefore, no process is adding files to orders/ while we are draining it.
    """

    logger.info("=" * 70)
    logger.info("SCRAPER COMPLETELY FINISHED.")
    logger.info("Starting processor drain phase...")
    logger.info("=" * 70)

    consecutive_empty_checks = 0

    while True:
        if processor_process is None:
            raise RuntimeError(
                "Processor process reference is missing while draining queue."
            )

        processor_exit_code = processor_process.poll()
        queue_files = get_queue_files()

        if processor_exit_code is not None:
            if queue_files:
                raise RuntimeError(
                    "Judgment processor exited before the queue was drained. "
                    f"exit_code={processor_exit_code}, "
                    f"remaining_files={len(queue_files)}"
                )

            if processor_exit_code != 0:
                raise RuntimeError(
                    "Judgment processor exited with an error while the queue "
                    f"was empty. exit_code={processor_exit_code}"
                )

            logger.info("Judgment processor exited cleanly and orders/ is empty.")
            return True

        if not queue_files:
            consecutive_empty_checks += 1

            logger.info(
                "orders/ is empty "
                f"({consecutive_empty_checks}/{QUEUE_EMPTY_CONFIRMATIONS})"
            )

            if consecutive_empty_checks >= QUEUE_EMPTY_CONFIRMATIONS:
                logger.info(
                    "Queue successfully drained. "
                    "Processor remained alive during confirmation period."
                )
                return True
        else:
            consecutive_empty_checks = 0

            pdf_count = sum(1 for f in queue_files if f.suffix.lower() == ".pdf")
            json_count = sum(1 for f in queue_files if f.suffix.lower() == ".json")

            logger.info(
                f"Queue pending: {pdf_count} PDF(s) + " f"{json_count} JSON file(s)."
            )

        time.sleep(QUEUE_CHECK_INTERVAL_SECONDS)


def cleanup_captcha_tmp():
    """
    Empty captcha-tmp after the scraper and processor have finished.

    The directory itself is preserved; only its contents are removed.
    Cleanup errors are logged as warnings so they do not hide the
    result of an otherwise successful scrape.
    """

    logger.info("=" * 70)
    logger.info("Cleaning captcha-tmp...")
    logger.info("=" * 70)

    if not CAPTCHA_TMP_DIR.exists():
        logger.info("captcha-tmp does not exist. Nothing to clean.")
        return

    removed = 0
    failed = 0

    for item in CAPTCHA_TMP_DIR.iterdir():
        try:
            if item.is_dir() and not item.is_symlink():
                shutil.rmtree(item)
            else:
                item.unlink()
            removed += 1
        except Exception:
            failed += 1
            logger.exception(f"Failed to remove captcha temp item: {item}")

    if failed:
        logger.warning(
            f"captcha-tmp cleanup completed with errors: "
            f"{removed} removed, {failed} failed."
        )
    else:
        logger.info(f"captcha-tmp cleanup complete. Removed {removed} item(s).")


# ============================================================
# PROCESS MANAGEMENT
# ============================================================


def start_processor():
    """
    Start judgment_processor.py.

    This function is called ONLY after download.py has completely exited.
    """

    global processor_process

    logger.info("=" * 70)
    logger.info("STARTING JUDGMENT PROCESSOR")
    logger.info(f"Script: {PROCESSOR_SCRIPT}")
    logger.info(f"Orders directory: {ORDERS_DIR}")
    logger.info("=" * 70)

    processor_process = subprocess.Popen(
        [sys.executable, str(PROCESSOR_SCRIPT)],
        cwd=str(BASE_DIR),
    )

    logger.info(
        f"Judgment processor started successfully (PID={processor_process.pid})"
    )

    # Give it one quick scheduling opportunity to catch immediate startup
    # failures, without introducing a fixed multi-second startup dependency.
    time.sleep(0.5)

    if processor_process.poll() is not None:
        raise RuntimeError(
            "Judgment processor exited immediately after startup. "
            f"exit_code={processor_process.returncode}"
        )


def start_scraper(
    court_codes,
    start_date,
    end_date,
    day_step,
    max_workers,
    dist_code,
    default_dist_codes,
    max_runtime_minutes,
    checkpoint_every,
    compress_pdfs,
):
    """
    Start download.py using its existing CLI.
    """

    global scraper_process

    command = [
        sys.executable,
        str(SCRAPER_SCRIPT),
        "--court_codes",
        court_codes,
        "--start_date",
        start_date,
        "--end_date",
        end_date,
        "--day_step",
        str(day_step),
        "--max_workers",
        str(max_workers),
    ]

    if dist_code:
        command.extend(
            [
                "--dist-code",
                str(dist_code),
            ]
        )

    if default_dist_codes:
        command.append("--default-dist-codes")
    else:
        command.append("--no-default-dist-codes")

    if max_runtime_minutes is not None:
        command.extend(
            [
                "--max-runtime-minutes",
                str(max_runtime_minutes),
            ]
        )

    if checkpoint_every is not None:
        command.extend(
            [
                "--checkpoint-every",
                str(checkpoint_every),
            ]
        )

    if compress_pdfs:
        command.append("--compress-pdfs")
    else:
        command.append("--no-compress-pdfs")

    logger.info("=" * 70)
    logger.info("Starting eCourts scraper...")
    logger.info("=" * 70)
    logger.info("Command:")
    logger.info(" ".join(command))
    logger.info("=" * 70)

    scraper_process = subprocess.Popen(
        command,
        cwd=str(BASE_DIR),
    )

    logger.info(f"Scraper started (PID={scraper_process.pid})")


def stop_processor():
    """
    Gracefully stop the judgment processor.

    On Windows, terminate() is the most reliable simple approach
    for a child Python process.
    """

    global processor_process

    if processor_process is None:
        return

    if processor_process.poll() is not None:
        logger.info(
            "Judgment processor already stopped "
            f"(exit code={processor_process.returncode})"
        )
        return

    logger.info("=" * 70)
    logger.info("Stopping judgment processor...")
    logger.info("=" * 70)

    processor_process.terminate()

    try:
        processor_process.wait(timeout=15)

        logger.info(
            "Judgment processor stopped " f"(exit code={processor_process.returncode})"
        )

    except subprocess.TimeoutExpired:
        logger.warning("Processor did not stop gracefully. " "Killing process...")

        processor_process.kill()
        processor_process.wait()

        logger.info("Judgment processor killed.")


def stop_scraper():
    """
    Stop scraper if it is still running.
    """

    global scraper_process

    if scraper_process is None:
        return

    if scraper_process.poll() is not None:
        return

    logger.warning("Stopping scraper...")

    scraper_process.terminate()

    try:
        scraper_process.wait(timeout=15)
    except subprocess.TimeoutExpired:
        logger.warning("Scraper did not stop gracefully. Killing...")
        scraper_process.kill()
        scraper_process.wait()


# ============================================================
# SIGNAL HANDLING
# ============================================================


def handle_shutdown(signum, frame):
    """
    Handle Ctrl+C / termination.
    """

    logger.warning("=" * 70)
    logger.warning("Shutdown signal received.")
    logger.warning("=" * 70)

    stop_scraper()
    stop_processor()

    sys.exit(1)


signal.signal(signal.SIGINT, handle_shutdown)
signal.signal(signal.SIGTERM, handle_shutdown)


def diagnose_orders_directory(label):
    """
    Diagnostic snapshot of orders/ contents.
    Does not modify anything.
    """

    logger.info("=" * 70)
    logger.info(f"ORDERS DIRECTORY DIAGNOSTIC — {label}")
    logger.info("=" * 70)

    logger.info(f"Orders directory: {ORDERS_DIR}")
    logger.info(f"Directory exists: {ORDERS_DIR.exists()}")

    if not ORDERS_DIR.exists():
        logger.error("CRITICAL: orders/ directory does not exist!")
        return

    try:
        all_files = [f for f in ORDERS_DIR.iterdir() if f.is_file()]

        pdf_files = [f for f in all_files if f.suffix.lower() == ".pdf"]

        json_files = [f for f in all_files if f.suffix.lower() == ".json"]

        other_files = [
            f for f in all_files if f.suffix.lower() not in {".pdf", ".json"}
        ]

        logger.info(f"Total files: {len(all_files)}")
        logger.info(f"PDF files: {len(pdf_files)}")
        logger.info(f"JSON files: {len(json_files)}")
        logger.info(f"Other files: {len(other_files)}")

        if not all_files:
            logger.warning("orders/ is EMPTY.")
            logger.warning(
                "Because the processor has not started yet, this means the "
                "scraper produced no PDF/JSON queue files."
            )

        else:
            logger.info("Queue files currently present:")

            for file in sorted(all_files)[:20]:
                try:
                    stat = file.stat()
                    logger.info(
                        f"  {file.name} | "
                        f"size={stat.st_size} bytes | "
                        f"mtime={stat.st_mtime}"
                    )
                except Exception:
                    logger.exception(f"Could not stat queue file: {file}")

            if len(all_files) > 20:
                logger.info(f"... and {len(all_files) - 20} more file(s)")

    except Exception:
        logger.exception("Failed to inspect orders/ directory.")

    logger.info("=" * 70)


# ============================================================
# ARGUMENTS
# ============================================================


def parse_args():
    parser = argparse.ArgumentParser(
        description=("NyaySaathi scraper + judgment processor orchestrator")
    )

    parser.add_argument(
        "--court_codes",
        required=True,
        help=("Comma-separated court codes. " 'Example: "2~5,3~1"'),
    )

    parser.add_argument(
        "--start_date",
        required=True,
        help="Start date in YYYY-MM-DD format",
    )

    parser.add_argument(
        "--end_date",
        required=True,
        help="End date in YYYY-MM-DD format",
    )

    parser.add_argument(
        "--day_step",
        type=int,
        default=1,
        help="Date step passed to download.py (default: 1)",
    )

    parser.add_argument(
        "--max_workers",
        type=int,
        default=2,
        help="Scraper worker count (default: 2)",
    )

    parser.add_argument(
        "--dist-code",
        default=None,
        help="Optional district code",
    )

    parser.add_argument(
        "--default-dist-codes",
        action="store_true",
        default=False,
        help="Use default district codes",
    )

    parser.add_argument(
        "--no-default-dist-codes",
        action="store_false",
        dest="default_dist_codes",
        help="Disable default district codes",
    )

    parser.add_argument(
        "--max-runtime-minutes",
        type=int,
        default=None,
        help="Optional scraper runtime limit",
    )

    parser.add_argument(
        "--checkpoint-every",
        type=int,
        default=None,
        help="Optional scraper checkpoint interval",
    )

    parser.add_argument(
        "--compress-pdfs",
        action="store_true",
        default=False,
        help="Enable PDF compression",
    )

    parser.add_argument(
        "--no-compress-pdfs",
        action="store_false",
        dest="compress_pdfs",
        help="Disable PDF compression",
    )

    return parser.parse_args()


# ============================================================
# MAIN
# ============================================================


def main():
    global scraper_process

    args = parse_args()

    logger.info("=" * 70)
    logger.info("NyaySaathi Pipeline Runner")
    logger.info("=" * 70)

    logger.info(f"Courts:       {args.court_codes}")
    logger.info(f"Start date:   {args.start_date}")
    logger.info(f"End date:     {args.end_date}")
    logger.info(f"Day step:     {args.day_step}")
    logger.info(f"Workers:      {args.max_workers}")
    logger.info(f"Orders dir:   {ORDERS_DIR}")
    logger.info("=" * 70)

    # --------------------------------------------------------
    # Make sure the queue directory exists.
    # --------------------------------------------------------

    ORDERS_DIR.mkdir(
        parents=True,
        exist_ok=True,
    )

    # Never silently hide files left by a previous run.
    # We do not delete them automatically.
    existing_queue = get_queue_files()

    if existing_queue:
        existing_pdf_count = sum(
            1 for f in existing_queue if f.suffix.lower() == ".pdf"
        )
        existing_json_count = sum(
            1 for f in existing_queue if f.suffix.lower() == ".json"
        )

        logger.warning("=" * 70)
        logger.warning("PRE-RUN QUEUE IS NOT EMPTY")
        logger.warning(
            f"Found {existing_pdf_count} PDF(s) + "
            f"{existing_json_count} JSON file(s) before scraper start."
        )
        logger.warning(
            "These files will remain in the queue and may be processed "
            "together with the current run. They are NOT being deleted."
        )
        logger.warning("=" * 70)
    else:
        logger.info("Pre-run check: orders/ is empty.")

    try:
        # ========================================================
        # PHASE 1 — SCRAPER
        # ========================================================
        #
        # The processor MUST NOT run during this phase.
        # download.py gets exclusive ownership of the orders/ output.
        # ========================================================

        logger.info("=" * 70)
        logger.info("PHASE 1/2 — SCRAPER")
        logger.info("Processor is NOT running.")
        logger.info("Waiting for download.py to completely finish.")
        logger.info("=" * 70)

        start_scraper(
            court_codes=args.court_codes,
            start_date=args.start_date,
            end_date=args.end_date,
            day_step=args.day_step,
            max_workers=args.max_workers,
            dist_code=args.dist_code,
            default_dist_codes=args.default_dist_codes,
            max_runtime_minutes=args.max_runtime_minutes,
            checkpoint_every=args.checkpoint_every,
            compress_pdfs=args.compress_pdfs,
        )

        logger.info("=" * 70)
        logger.info("WAITING FOR SCRAPER PROCESS TO EXIT")
        logger.info(f"Scraper PID: {scraper_process.pid}")
        logger.info("=" * 70)

        scraper_exit_code = scraper_process.wait()

        logger.info("=" * 70)
        logger.info(f"SCRAPER PROCESS EXITED — exit_code={scraper_exit_code}")
        logger.info("=" * 70)

        # If scraper failed, DO NOT start the processor. This makes the
        # scraper -> queue -> processor boundary explicit and deterministic.
        if scraper_exit_code != 0:
            logger.error("=" * 70)
            logger.error("SCRAPER FAILED")
            logger.error(f"download.py exited with code {scraper_exit_code}.")
            logger.error("Judgment processor will NOT be started.")
            logger.error("=" * 70)

            diagnose_orders_directory("AFTER SCRAPER FAILURE")

            return scraper_exit_code

        # At this point download.py's OS process has fully exited.
        # No scraper process remains that can write to orders/.
        diagnose_orders_directory("AFTER SCRAPER COMPLETELY FINISHED")

        logger.info("=" * 70)
        logger.info("SCRAPER PHASE COMPLETE")
        logger.info(
            "The scraper process has exited successfully. "
            "No processor has touched orders/."
        )
        logger.info("=" * 70)

        # ========================================================
        # PHASE 2 — PROCESSOR
        # ========================================================
        #
        # Processor starts ONLY after scraper process completion.
        # ========================================================

        start_processor()

        wait_for_queue_to_drain()

        diagnose_orders_directory("AFTER PROCESSOR DRAIN")

        # If files remain, never report success.
        remaining_files = get_queue_files()
        if remaining_files:
            raise RuntimeError(
                "Processor drain reported success but files remain in orders/: "
                f"{len(remaining_files)} file(s)."
            )

        # ========================================================
        # PHASE 3 — SHUTDOWN / CLEANUP
        # ========================================================

        stop_processor()

        processor_exit_code = (
            processor_process.returncode if processor_process is not None else None
        )

        logger.info(f"Processor final exit code: {processor_exit_code}")

        cleanup_captcha_tmp()

        logger.info("=" * 70)
        logger.info("PIPELINE SUCCESS")
        logger.info("Scraper completed before processor started.")
        logger.info("Processor drained the completed scraper output.")
        logger.info("orders/ is empty.")
        logger.info("=" * 70)

        return 0

    except KeyboardInterrupt:
        logger.warning("Interrupted by user.")
        stop_scraper()
        stop_processor()
        return 1

    except Exception:
        logger.exception("Pipeline failed unexpectedly.")

        stop_scraper()
        stop_processor()

        return 1


if __name__ == "__main__":
    sys.exit(main())
