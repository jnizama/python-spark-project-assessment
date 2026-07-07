"""
main.py
Entry point for the Sales Data Processing Application.
"""

import logging
from pipeline import SalesPipeline

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

def main() -> None:
    """
    Execute the Sales Data pipeline.
    """

    pipeline = SalesPipeline()
    try:
        pipeline.run()

    except Exception:
        logger.exception("Pipeline execution failed.")
        raise
    finally:
        pipeline.close()

if __name__ == "__main__":
    main()