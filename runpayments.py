import subprocess
import sys
import logging
import libs
import urllib.parse
import requests

config = libs.load_config_from_file('config.json')
logger = libs.setup_logger(log_file="l0ps.log", log_level=logging.DEBUG, name="runpayments")
telegram_bot_url = f"https://api.telegram.org/bot{config['telegrambottoken']}"

print("Claiming wavesdaolp")
result = subprocess.run(["poetry", "run", "python", "claimwavesdaolp.py"], capture_output=True, text=True)
if result.returncode == 1:
    print("Claiming wavesdaolp failed, check log file")
    message = urllib.parse.quote(f"Project: {config['projectname']}: Claiming wavesdaolp failed")
    requests.get(f"{telegram_bot_url}/sendmessage?chat_id={config['telegramchat_id']}&text={message}&parse_mode=HTML")
    sys.exit(1)

print("Calculating payments")
result = subprocess.run(["poetry", "run", "python", "calculatepayments.py", "N", "N"], capture_output=True, text=True)
if result.returncode == 1:
    print("Calculating payments failed, check log file")
    message = urllib.parse.quote(f"Project: {config['projectname']}: Calculating payments failed")
    requests.get(f"{telegram_bot_url}/sendmessage?chat_id={config['telegramchat_id']}&text={message}&parse_mode=HTML")
    sys.exit(1)

print("Sending payments")
result = subprocess.run(["poetry", "run", "python", "sendpayments.py", "N"], capture_output=True, text=True)
if result.returncode == 1:
    print("Sending payments failed, check log file")
    message = urllib.parse.quote(f"Project: {config['projectname']}: Sending payments failed")
    requests.get(f"{telegram_bot_url}/sendmessage?chat_id={config['telegramchat_id']}&text={message}&parse_mode=HTML")
    sys.exit(1)
else:
    print("Payment completed succesfully")

message = urllib.parse.quote(f"Project: {config['projectname']}: Completed payments succesfully.")
requests.get(f"{telegram_bot_url}/sendmessage?chat_id={config['telegramchat_id']}&text={message}&parse_mode=HTML")
sys.exit(0);
    
