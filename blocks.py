import sys
import pywaves as pw
import time
import sqlite3
import libs
import logging
import traceback
import json
import urllib3
from typing import Optional, List, Dict, Any

TRANSACTION_TYPES = {
    'LEASE': 8,
    'LEASE_CANCEL': 9,
    'INVOKE': 16,
    'INVOKE_SCRIPT': 18
}

def getallblocks(conn: sqlite3.Connection, startblock: Optional[int], endblock: Optional[int]) -> None:
    """
    Get blocks from startblock to endblock and analyze leases, unleases, and rewards.
    
    Args:
        conn: SQLite database connection
        startblock: Starting block height (inclusive)
        endblock: Ending block height (inclusive)
        
    Raises:
        sqlite3.Error: If database operations fail
        Exception: If API calls fail
    """
        
    global config, logger
    height = libs.height(config['waves']['node'])
    logger.info(f"Height: {height}")

    _startblock = startblock
    _endblock = endblock    
    
    # If not specified, go incremental.
    
    if _endblock is None:
        _endblock = height - 1
    
    if _startblock is None:
        # Load from 1 block before
        cursor = conn.cursor()
        cursor.execute(f"SELECT MAX(height) + 1 AS startblock FROM waves_blocks")
        row = cursor.fetchone()
        _startblock = row[0] if row[0] else 1 #if row[0] is none, start from block 1
        cursor.close()

    logger.info(f"Loading Blocks from {_startblock} to {_endblock}")

    steps = 100
    totalsavedblocks = 0
    while _startblock < _endblock:
        currentblocks = []
        if _startblock + (steps - 1) < _endblock:
            logger.info("Getting blocks from %d to %d" % ( _startblock, _startblock + (steps - 1)))                        
            res = libs.wrapper(config['waves']['node'], '/blocks/seq/%d/%d' % (_startblock, _startblock + (steps - 1)))
            if res is not None:
                currentblocks = res
            else:
                raise Exception('CURL error while fetching blocks.')
        else:
            logger.info("Getting blocks from %d to %d" % ( _startblock, _endblock))
            res = libs.wrapper(config['waves']['node'], '/blocks/seq/%d/%d' % (_startblock, _endblock))
            if res is not None:
                currentblocks = res
            else:
                raise Exception('CURL error while fetching blocks.')

        time.sleep(1)

        # Collect all relevant transaction ids from all currentblocks
        tx_ids = []
        for block in currentblocks:
            tx_ids.extend([tx['id'] for tx in block['transactions'] if tx['type'] in (
                TRANSACTION_TYPES['LEASE_CANCEL'],
                TRANSACTION_TYPES['INVOKE'],
                TRANSACTION_TYPES['INVOKE_SCRIPT']
            )])

        # Fetch extended tx info
        extended_map = {}
        extended_transactions = libs.tx_bulk(config['waves']['node'], tx_ids)
        logger.debug(f"Found {len(tx_ids)} txs")
        extended_map.update({tx['id']: tx for tx in extended_transactions})

        # Process blocks and transactions
        cursor = conn.cursor()
        for block in currentblocks:            
            total_tx16calls = 0
            for transaction in block['transactions']:                
                if transaction['type'] in (
                    TRANSACTION_TYPES['LEASE'],
                    TRANSACTION_TYPES['LEASE_CANCEL'],
                    TRANSACTION_TYPES['INVOKE'],
                    TRANSACTION_TYPES['INVOKE_SCRIPT']
                ):
                    extended_tx = extended_map.get(transaction['id'])
                    tx16calls = checkandsave_leasetransaction(conn, block, transaction, extended_tx)
                    total_tx16calls += tx16calls
            # save block data
            sql = f"""
                REPLACE INTO waves_blocks ( height, generator, fees, txs, timestamp, tx16calls)
                VALUES (
                    {block['height']},
                    '{block['generator']}',
                    {block['totalFee'] },
                    {len(block['transactions'])},
                    {block['timestamp'] // 1000},
                    {total_tx16calls}
                )"""
            
            cursor.execute(sql)    
        cursor.close()
        
        if _startblock + steps < _endblock:
            _startblock += steps
        else:
            _startblock = _endblock

        totalsavedblocks += len(currentblocks)
        logger.info(f"Total Blocks Loaded: {totalsavedblocks}, committing...")
        try:
            with conn:  # This ensures proper transaction handling
                conn.commit()
        except sqlite3.Error as e:
            logger.error(f"Database error: {e}")
            raise
        time.sleep(1)
        
def checkandsave_leasetransaction(conn, block, transaction, extendedtransaction):
    """
    Check block for lease and unleases
    """

    global config, logger
        
    tx16calls = 0

    if ('type' in transaction and transaction['type'] == TRANSACTION_TYPES['LEASE'] and (
        transaction['recipient'] == config['waves']['generatoraddress']
        or transaction['recipient'] == "address:" + config['waves']['generatoraddress']
        or transaction['recipient'] == "alias:W:" + config['waves']['generatoralias']
    )):
        logger.debug(f"Block {block['height']}: found a lease from {transaction['sender']}, saving, id: {transaction['id']}")
        cursor = conn.cursor()
        cursor.execute(
            """
            REPLACE INTO waves_leases (tx_id, lease_id, txtype, address, start, leasedate, end, amount)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                transaction['id'],
                transaction['id'],
                transaction['type'],
                transaction['sender'],
                block['height'],
                transaction['timestamp'] // 1000,
                None,
                transaction['amount'],
            ),
        )
        cursor.close()
    elif 'type' in transaction and transaction['type'] == TRANSACTION_TYPES['LEASE_CANCEL']:
        if extendedtransaction['lease']['recipient'] == config['waves']['generatoraddress']:
            cursor = conn.cursor()
            sql = f"""
                UPDATE waves_leases
                SET end = {extendedtransaction['height']},
                    endleasedate = {extendedtransaction['timestamp'] // 1000}
                WHERE lease_id = '{extendedtransaction['leaseId']}'
            """
            cursor.execute(sql)
            if cursor.rowcount > 0:
                logger.debug(f"Block: {extendedtransaction['height']}: Found a lease cancellation,... id: {extendedtransaction['leaseId']}")
            cursor.close()

    elif 'type' in transaction and (transaction['type'] == TRANSACTION_TYPES['INVOKE'] or 
                                  transaction['type'] == TRANSACTION_TYPES['INVOKE_SCRIPT']):
        leases = []
        leasecancels = []
        
        # update tx16 counter where sender is generator address
        if transaction['sender'] == config['waves']['generatoraddress']:
            tx16calls = tx16calls + 1            

        # Check recursively invokes for leases and lease cancels
        # logger.info(f"Analyzing tx {transaction['id']} type {transaction['type']}")

        if transaction['type'] == TRANSACTION_TYPES['INVOKE']:
            if 'stateChanges' in extendedtransaction and extendedtransaction['stateChanges'] is not None:
                analyzestatechanges(extendedtransaction['stateChanges'], leases, leasecancels)
        elif transaction['type'] == TRANSACTION_TYPES['INVOKE_SCRIPT']:
            if 'stateChanges' in extendedtransaction['payload'] and extendedtransaction['payload']['stateChanges'] is not None:
                analyzestatechanges(extendedtransaction['payload']['stateChanges'], leases, leasecancels)
	  
        #logger.info(f"Tx {extendedtransaction['id']} has {len(leases)} leases and {len(leasecancels)} lease cancels.")

        # Save leases
        for lease in leases:
            if (
                lease['recipient'] == config['waves']['generatoraddress'] or
                lease['recipient'] == "address:" + config['waves']['generatoraddress'] or
                lease['recipient'] == "alias:W:" + config['waves']['generatoralias']
            ):
                logger.debug(f"Block: {extendedtransaction['height']}: Found a lease... id: {lease['id']}, saving it.")
                try:
                    cursor = conn.cursor()
                    cursor.execute(
                        """
                        REPLACE INTO waves_leases (tx_id, lease_id, txtype, address, start, leasedate, end, amount)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            transaction['id'],
                            lease['id'],
                            transaction['type'],
                            lease['sender'],
                            block['height'],
                            transaction['timestamp'] // 1000,
                            None,
                            lease['amount'],
                        )
                    )
                    conn.commit()
                except sqlite3.Error as e:
                    logger.error(f"Error saving lease: {e}")
        # Save Cancel Lease
        for leasecancel in leasecancels:
            try:
                cursor = conn.cursor()
                cursor.execute(
                    f"""
                        UPDATE waves_leases
                        SET end = {extendedtransaction['height']},
                            endleasedate = {extendedtransaction['timestamp']//1000}
                        WHERE lease_id = '{leasecancel['id']}'
                    """
                )
                conn.commit()
                if cursor.rowcount > 0:
                    logger.debug(f"Block: {extendedtransaction['height']}: Found a lease cancellation... id: {leasecancel['id']}")
            except sqlite3.Error as e:
                logger.error(f"Error updating lease cancellation: {e}")
    
    return tx16calls

def analyzestatechanges(statechanges, leases, leasecancels):
    """
    Analyzes the state changes of a transaction and extracts lease and lease cancellation information.
    """

    if 'leases' in statechanges and isinstance(statechanges['leases'], list) and statechanges['leases']:
        leases.extend(statechanges['leases'])

    if 'leaseCancels' in statechanges and isinstance(statechanges['leaseCancels'], list) and statechanges['leaseCancels']:
        leasecancels.extend(statechanges['leaseCancels'])

    if 'invokes' in statechanges and isinstance(statechanges['invokes'], list) and statechanges['invokes']:
        for invoke in statechanges['invokes']:
            if 'stateChanges' in invoke:
                analyzestatechanges(invoke['stateChanges'], leases, leasecancels)
# main

config = None
logger = None

def main():

    global config, logger
    logger = libs.setup_logger(log_file="l0ps.log", log_level=logging.DEBUG, name="blocks")

    if len(sys.argv) < 1:
        print("Usage: poetry run python blocks [startblock] [endblock]")
        sys.exit(1)

    startblock = None
    if len(sys.argv) > 1:
        try:
            startblock = int(sys.argv[1])
        except ValueError:
            logger.debug("Error: startblock must be an integer.")
            sys.exit(1)

    endblock = None
    if len(sys.argv) > 2:
        try:
            endblock = int(sys.argv[2])
        except ValueError:
            logger.debug("Error: endblock must be an integer.")
            sys.exit(1)

    try:
        config = libs.load_config_from_file('config.json')
        conn = sqlite3.connect(config['database'])  
        logger.info("Loading Blocks");
        validate_block_range(startblock, endblock)
        getallblocks(conn, startblock, endblock)
    except Exception as e:
        logger.debug("Error: %s", e)
        logger.error(traceback.format_exc())
        sys.exit(1)

def validate_block_range(startblock, endblock):
    if startblock is not None and startblock < 1:
        raise ValueError("Start block must be greater than 0")
    if endblock is not None and endblock < startblock:
        raise ValueError("End block must be greater than start block")

if __name__ == "__main__":
    main()
