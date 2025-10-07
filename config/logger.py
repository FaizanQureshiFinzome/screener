import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,  # Set to DEBUG if you want more detailed logs
    format='[%(asctime)s] %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

logger = logging.getLogger(__name__)
