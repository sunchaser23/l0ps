import sys
import json
import requests
import pywaves as pw
import logging
import os
import libs

def main():
    if len(sys.argv) != 1:
        print("Usage: python waves_claimwavesdaolp.py")
        sys.exit(1)

    config = libs.load_config_from_file('config.json')
    logger = libs.setup_logger(log_file="l0ps.log", log_level=logging.DEBUG, name="claimwavesdaolp")
    pw.setNode(config['waves']['node'], config['waves']['chain']);
    addr = pw.address.Address(privateKey=config['waves']['pk']);

    logger.info(f"Operating from address: {addr.address}");

    dappaddr = pw.address.Address(config['waves']['claimwavesdaolpdappaddress'])
    addr.invokeScript(dappaddr.address, 'processBlocks')

if __name__ == "__main__":
    main()
