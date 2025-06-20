import sys
import pywaves as pw
import libs
import logging
import traceback
from pprint import pprint

def main():
    if len(sys.argv) != 1:
        print("Usage: poetry run python claimwavesdaolp.py")
        sys.exit(1)

try:
    config = libs.load_config_from_file('config.json')
    logger = libs.setup_logger(log_file="l0ps.log", log_level=logging.DEBUG, name="claimwavesdaolp")
    pw.setNode(config['waves']['node'], config['waves']['chain']);
    addr = pw.address.Address(privateKey=config['waves']['pk']);

    logger.info(f"Operating from address: {addr.address}");

    dappaddr = pw.address.Address(config['waves']['claimwavesdaolpdappaddress'])
    addr.invokeScript(dappaddr.address, 'processBlocks')
    tx = addr.invokeScript(dappaddr.address, 'claimLP')    
    if ('error' in tx):
        if 'nothing to claim' in tx['message']:
            logger.info("No LP to claim")
        else:
            raise Exception(f"Error: {tx['message']}")
    else:
        pw.waitFor(tx['id'])
except Exception as e:
    logger.error(f"Error: {e}")
    logger.error(traceback.format_exc())
    sys.exit(1)

if __name__ == "__main__":
    main()
