import logging
import os
import requests
import json

def height(host):
    res = wrapper(host, '/blocks/height')
    if res is not False:
        return res['height']

def get_balances(config, addr):
    balances = {}
    balances['waves'] = {
        'balance':addr.balance(),
        'assetid':None,
        'decimals':8
    }
    for token, details in config['waves']['airdrops'].items():
        if details['enabled']:
            balances[token] = {
                'balance':addr.balance(assetId=details['assetid']),
                'assetid':details['assetid'],
                'decimals':details['decimals']
            }

    return balances

def setup_logger(log_file="app.log", log_level=logging.INFO, name=__name__):

    # Ensure the directory exists
    log_dir = os.path.dirname(log_file)
    if log_dir and not os.path.exists(log_dir):
        os.makedirs(log_dir)

    logger = logging.getLogger(name)
    logger.setLevel(log_level)

    # Create a file handler
    file_handler = logging.FileHandler(log_file)
    file_handler.setLevel(log_level)

    # Create a console handler (optional)
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO) # info to the console

    # Create a formatter
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(formatter)
    console_handler.setFormatter(formatter)

    # Add handlers to the logger
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    return logger

def wrapper(host, api, postData='', headers=''):

    if postData:
        req = requests.post('%s%s' % (host, api), data=postData, headers={'content-type': 'application/json'})
    else:
        req = requests.get('%s%s' % (host, api), headers=headers)
    try:
        #print(req)
        return req.json()
    except json.JSONDecodeError as e:
        print(f"> JSON Decode error: {e}")
        return None

def blockchainrewards(host):
    """Gets blockchain reward"""
    res = wrapper(host, f"/blockchain/rewards")
    if res is not False:
        return res

def tx(host, tx_id):
    """Gets a transaction by its ID."""
    res = wrapper(host, f"/transactions/info/{tx_id}")
    if res is not False:
        return res

def encrypt_decrypt(mode, password, encrypted_key):
    """Encrypts/decrypts a key."""
    key = Fernet.generate_key() #generate a key, or load a key from a file.
    f = Fernet(key)

    if mode == 'decrypt':
        try:
            decrypted_key = f.decrypt(encrypted_key.encode()).decode()
            return decrypted_key
        except Exception as e:
            print(f"Decryption error: {e}")
            sys.exit(1)
    elif mode == 'encrypt':
        try:
            encrypted_key = f.encrypt(password.encode()).decode()
            return encrypted_key
        except Exception as e:
            print(f"Encryption error: {e}")
            sys.exit(1)
    else:
        print("Error: Invalid mode for encrypt_decrypt.")
        sys.exit(1)

def load_config_from_file(filepath):
    try:
        with open(filepath, 'r') as f:
            config = json.load(f)
            return config
    except FileNotFoundError:
        print(f"Error: Configuration file not found at {filepath}")
        sys.exit(1)
    except json.JSONDecodeError:
        print(f"Error: Invalid JSON format in {filepath}")
        sys.exit(1)
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        sys.exit(1)

