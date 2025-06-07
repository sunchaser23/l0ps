import subprocess
import sys
import logging
import libs
import urllib.parse
import requests


config = libs.load_config_from_file('config.json')
logger = libs.setup_logger(log_file="l0ps.log", log_level=logging.DEBUG, name="runpayments")
telegram_bot_url = f"https://api.telegram.org/bot{config['telegrambottoken']}"

logger.info("Claiming wavesdaolp")
result = subprocess.run(["poetry", "run", "python", "claimwavesdaolp.py"], capture_output=True, text=True)
if result.returncode == 1:
    logger.error("Claiming wavesdaolp failed")
    message = urllib.parse.quote(f"Project: {config['projectname']}: Claiming wavesdaolp failed")
    requests.get(f"{telegram_bot_url}/sendmessage?chat_id={config['telegramchat_id']}&text={message}&parse_mode=HTML")
    sys.exit(1)

logger.info("Calculating payments")
result = subprocess.run(["poetry", "run", "python", "calculatepayments.py", "N"], capture_output=True, text=True)
if result.returncode == 1:
    logger.error("Calculating payments failed")
    message = urllib.parse.quote(f"Project: {config['projectname']}: Calculating payments failed")
    requests.get(f"{telegram_bot_url}/sendmessage?chat_id={config['telegramchat_id']}&text={message}&parse_mode=HTML")
    sys.exit(1)

logger.info("Sending payments")
result = subprocess.run(["poetry", "run", "python", "sendpayments.py", "N"], capture_output=True, text=True)
if result.returncode == 1:
    logger.error("Sending payments failed")
    message = urllib.parse.quote(f"Project: {config['projectname']}: Sending payments failed")
    requests.get(f"{telegram_bot_url}/sendmessage?chat_id={config['telegramchat_id']}&text={message}&parse_mode=HTML")
    sys.exit(1)

    