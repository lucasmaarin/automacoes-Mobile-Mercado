import os
from dotenv import load_dotenv
import logging

load_dotenv()

_log_handlers = [logging.StreamHandler()]
if os.getenv('LOG_TO_FILE', '').lower() in ('1', 'true', 'yes'):
    _log_handlers.append(logging.FileHandler('app.log'))
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=_log_handlers
)
logger = logging.getLogger(__name__)

SECRET_KEY = os.getenv('SECRET_KEY', 'dev-secret-key-change-in-prod')
