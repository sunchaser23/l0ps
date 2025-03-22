import sys
import pywaves as pw
import time
import sqlite3
import libs
import logging 
import traceback
import json

def getallblocks(conn, startblock, endblock):
    global config, logger
    height = pw.height()
    logger.debug(f"Height: {height}")

    _startblock = startblock
    _endblock = endblock

    # If not specified, go incremental.
    if _startblock is None and _endblock is None:
        # Get 1 block before so they are complete.
        _endblock = height - 1
        cursor = conn.cursor()
        cursor.execute(f"SELECT MAX(height) + 1 AS startblock FROM waves_blocks")
        row = cursor.fetchone()
        _startblock = row[0] if row[0] else 1 #if row[0] is none, start from block 1
        cursor.close()

    steps = 100
    totalsavedblocks = 0
    while _startblock < _endblock:
        currentblocks = []
        if _startblock + (steps - 1) < _endblock:
            logger.info("Getting blocks from %d to %d" % ( _startblock, _startblock + (steps - 1)))
            res = libs.wrapper(config['waves']['node'], '/blocks/seq/%d/%d' % (_startblock, _startblock + (steps - 1)))
            while res is False:
                logger.debug('Got error from CURL, retrying in 5 secs...')
                time.sleep(5)
                res = libs.wrapper(config['waves']['node'], '/blocks/seq/%d/%d' % (_startblock, _startblock + (steps - 1)))
            if res is not False:
                currentblocks = res
            else:
                raise Exception('CURL error while fetching blocks.')
        else:
            logger.info("Getting blocks from %d to %d" % ( _startblock, _startblock + (steps - 1)))
            res = libs.wrapper(config['waves']['node'], '/blocks/seq/%d/%d' % (_startblock, _endblock))
            while res is False:
                logger.debug('Got error from CURL, retrying in 5 secs...')
                time.sleep(5)
                res = libs.wrapper(config['waves']['node'], '/blocks/seq/%d/%d' % (_startblock, _endblock))
            if res is not False:
                currentblocks = res
            else:
                raise Exception('CURL error while fetching blocks.')

        time.sleep(1)

        # Process blocks and transactions.
        for block in currentblocks:
            for transaction in block['transactions']:
                if transaction['type'] in (8, 9, 16):
                    checkandsave_leasetransaction(conn, block, transaction)
                else:
                    pass
        # Saving Blocks...
        logger.debug(f"Saving Block Data")
        cursor = conn.cursor()
        for block in currentblocks:
            sql = f"""
                REPLACE INTO waves_blocks ( height, generator, fees, txs, timestamp )
                VALUES (
                    {block['height']},
                    '{block['generator']}',
                    {block['totalFee'] },
                    {len(block['transactions'])},
                    {block['timestamp'] // 1000}
                )"""
            cursor.execute(sql)
        cursor.close()

        if _startblock + steps < _endblock:
            _startblock += steps
        else:
            _startblock = _endblock

        totalsavedblocks += steps

        if totalsavedblocks % steps == 0:
            logger.debug(f"Total Blocks Loaded: {totalsavedblocks}, committing...")
            conn.commit()

        time.sleep(1)  # Convert microseconds to seconds

def checkandsave_leasetransaction(conn, block, transaction):

    global config, logger
    #logger.debug(transaction)
    if ('type' in transaction and transaction['type'] == 8 and (
        transaction['recipient'] == config['waves']['generatoraddress']
        or transaction['recipient'] == "address:" + config['waves']['generatoraddress']
        or transaction['recipient'] == "alias:W:" + config['waves']['generatoralias']
    )):
        logger.debug(f"Block {block['height']}: found a lease to node, saving, id: {transaction['id']}")
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
    elif 'type' in transaction and transaction['type'] == 9:
        extendedtransaction = libs.tx(config['waves']['node'], transaction['id'])
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

def checkleases(conn):
    """
    Checks active leases between the blockchain and the local database.

    Args:
        conn (sqlite3.Connection): SQLite3 database connection.
    """

    global logger, config

    height = pw.height()
    logger.info(f"height: {height}")
    logger.info('Finding active leases on blockchain...')

    res = libs.wrapper(config['waves']['node'], f"leasing/active/{config['waves']['generatoraddress']}")
    while res is False:
        logger.warning('Error while fetching active leases, retrying...')
        time.sleep(5)
        res = libs.wrapper(config['waves']['node'], f"leasing/active/{config['waves']['generatoraddress']}")

    if res is not False:
        activeleases = res

    logger.info(res)
    exit(1)
    logger.info('Finding active leases on local db...')

    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM waves_leases WHERE end IS NULL ORDER BY amount DESC, start ASC")
    rows = cursor.fetchall()

    leases = {}
    amount = 0
    for row in rows:
        leases[row['tx_id']] = {  
            'address': row['address'],  
            'amount': row['amount'],  
            'height': row['start'],  
            'end': row['end'],  
        }
        amount += row['amount']

    logger.info(f"Active leases on DB: {len(leases)}")
    logger.info(f"Total amount: {amount / (10 ** 8)}")
    logger.info("Checking for non-cancelled leases...")

    logger.debug(leases)
    logger.debug(activeleases)
    for id, data in leases.items():
        found = False
        if activeleases != None:
            for activelease in activeleases:
                logger.info(f"Id: {id}, activeleases id: {activelease['id']}")
                if id == activelease['id']:
                    found = True
                    break

        if not found:
            if data['height'] + 1000 > height:
                pass
            else:
                logger.warning(f"Lease {id} not confirmed, amount: {data['amount'] / (10 ** 8)} needs to be closed")
                tx = libs.tx(config['waves']['node'], id)
                sql = f"UPDATE waves_leases SET endleasedate = {int(tx['timestamp'] / 1000)}, end = {tx['height']} WHERE tx_id = '{id}'"

                if dryrun == 'N':
                    try:
                        cursor.execute(sql)
                        conn.commit()
                    except sqlite3.Error as e:
                        logger.error(f"SQLite Error: {e}")
                else:
                    logger.debug(sql)

    logger.info('Checking for active leases not present on DB...')

    for activelease in activeleases:
        if activelease['id'] not in leases or leases[activelease['id']]['end'] is not None:
            logger.warning(f"Lease {activelease['id']} is not registered in DB.")
            tx = libs.tx(config['waves']['node'], activelease['id'])
            sql = f"""
                REPLACE INTO waves_leases (tx_id, lease_id, txtype, address, start, leasedate, amount)
                VALUES ('{activelease['id']}', '{activelease['id']}', '{tx['type']}', '{activelease['sender']}', {activelease['height']}, {int(tx['timestamp'] / 1000)}, {activelease['amount']})
            """

            try:
                cursor.execute(sql)
                conn.commit()
            except sqlite3.Error as e:
                logger.error(f"SQLite Error: {e}")
            else:
                logger.debug(sql)

    cursor.close()

# main

config = None
logger = None

def main():

    global config, logger

    logger = libs.setup_logger(log_file="l0ps.log", log_level=logging.DEBUG, name="blocks")

    if len(sys.argv) < 1:
        print("Usage: python3 blocks [startblock] [endblock]")
        sys.exit(1)  # Exit with a non-zero code to indicate an error

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
        conn = sqlite3.connect(config['database'])  # Use the database filename from config
        logger.info("Loading Blocks");
        getallblocks(conn, startblock, endblock)
        checkleases(conn)
    except Exception as e:
        logger.debug("Error: %s", e)
        logger.error(traceback.format_exc())
        sys.exit(1) #exit with an error.

if __name__ == "__main__":
    main()
