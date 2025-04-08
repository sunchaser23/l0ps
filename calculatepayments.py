import sys
import json
import requests
import pywaves as pw
import logging
import libs
import sqlite3
import datetime

def savepayments(config, conn, payments, blocksinfo, totals, dryrun):
    global logger

    # insert payment entry
    now = datetime.datetime.now()
    sql = """
        INSERT INTO waves_payments (startblock, endblock, minedblocks, summary, paymentlock, timestamp) VALUES ( ?, ?, ?, ?, ?, ?)
        """
    try:
        cursor = conn.cursor()
        cursor.execute(sql, (blocksinfo['startblock']+1, blocksinfo['endblock'], blocksinfo['minedblocks'], json.dumps(totals), 'Y', now))
        payment_id = cursor.lastrowid

        for address, tokens in payments.items():
            for token, paymentdetails in tokens.items():
                sql = """
                    INSERT OR REPLACE INTO waves_paymentdetails (payment_id, address, status, token, token_id, amount)
                    VALUES (?, ?, ?, ?, ?, ?)
                """
                cursor.execute(sql, (payment_id, address, 'new', token, paymentdetails['id'], paymentdetails['reward']))
        if (dryrun == 'Y'):
            logger.info("Dryrun mode, rollbacking.")
            conn.rollback()
        else:
            conn.commit()
    except sqlite3.Error as e:
        logger.error(f"SQLLite error: {e}")
        conn.rollback()
    finally:
        conn.close()


def getwavesactiveleasesatblock(height, leases_x_id):

    activeleasesinfo = {'total':0}

    # Collect leases within the window of interest (last 1000 blocks)
    # This manages single leases

    lower_bound = height - 1000
    grouped_by_address = {}

    for lease in leases_x_id.values():
        lease_id = lease[0]
        address = lease[3]
        start = lease[4]
        end = lease[7] if lease[7] is not None else float('inf')
        amount = lease[8]

        # group leases by address
        if address not in grouped_by_address:
            grouped_by_address[address] = []
        grouped_by_address[address].append((lease[0], start, end, amount))

    # This manages continuous leases (leases that are closed and reopened in the same block)

    for address, leases in grouped_by_address.items():
        intervals = []
        # foreach lease of the address add their interval 
        for lease_id, start, end, amount in leases:
            intervals.append((lease_id, amount, start, end))

        merged = intervals[:]
  
        # we check every leases for the address and at the present height 
        # we are searhing for leases that starts on the same block when another leases was canceled 
        # we consider them as "merged/continuous" we merge them and we keep as amount the amount 
        # of the last lease in the chain
        # 
        # non continuous leases are kept as they are
        #        
        # example: 
        # lease1 amount: 20 [start-end]: [3000 - 4500]
        # lease2 amount: 10 [start-end]: [4500 - 8000]
        #
        # becomes:
        # lease1,2 amount: 10 [start-end]: [3000 - 8000] 
        # 
        # The result: at block 5000 lease2 generation amount is calculated as 10
        #

        logger.debug(f"LEASES: {height} {address} {merged}")

        i = 0
        while i < len(merged):
            j = 0
            while j < len(merged):
                if i != j and merged[i][3] == merged[j][2]:  # Check if end_block matches start_block
                    # Merge continuous leases by updating the end_block and amount_leased
                    merged_ids = merged[i][0] if isinstance(merged[i][0], list) else [merged[i][0]]
                    merged_ids.append(merged[j][0])  # Add the lease_id of the second lease
                    merged[i] = (
                        merged_ids,  # Keep track of all merged lease_ids
                        merged[j][1],  # Update amount_leased to the last lease's amount
                        merged[i][2],  # Keep the start_block of the first lease
                        merged[j][3]   # Update end_block to the last lease's end_block
                    )
                    merged.pop(j)  # Remove the merged lease
                    if j < i:
                        i -= 1  # Adjust index if a previous lease was removed
                    j = 0  # Restart inner loop to check for further merges
                else:
                    j += 1
            i += 1

        # Filter out entries with start or end outside bounds
        merged = [lease for lease in merged if lease[2] < lower_bound and lease[3] > height]

        logger.debug(f"MERGED: {height} {address} {merged}")

        if not merged:
            activeleasesinfo['leases'] = {address: {'total': 0}}
        for lease in merged:
            lease_ids, amount = lease[0], lease[1]
            if not isinstance(lease_ids, list):
                lease_ids = [lease_ids]  # Ensure lease_ids is a list

            if 'leases' not in activeleasesinfo:
                activeleasesinfo['leases'] = {}
            if address not in activeleasesinfo['leases']:
                activeleasesinfo['leases'][address] = {'total': 0}

            activeleasesinfo['leases'][address]['total'] += amount
            activeleasesinfo['total'] += amount

        logger.debug(f"ACTIVE: {height} {address} {activeleasesinfo}")

    return activeleasesinfo

def distribute(config, blocksinfo, balances, leases_x_id):
    airdroprewards = {}
    leasersairdroprewards = {}
    nodeownerairdroprewards = {}
    payments = {}

    # for airdrops, reward is always balance / forgedblocks
    for token, details in config['waves']['airdrops'].items():
        if details['enabled']:
            if token in balances:
                airdroprewards[token]=int(balances[token]['balance']/blocksinfo['minedblocks'])
                leasersairdroprewards[token]=airdroprewards[token]*(int(config['waves']['percentagetodistribute'])/100)
                nodeownerairdroprewards[token]=airdroprewards[token]-leasersairdroprewards[token]
            else:
                airdroprewards[token] = 0
                leasersairdroprewards[token] = 0
                nodeownerairdroprewards[token] = 0

    previousblockinfo = blocksinfo['startblock']

    # Find out current block reward
    res = libs.blockchainrewards(config['waves']['node'])
    blockrewards = res['currentReward'] / 3

    for height, blockinfo in blocksinfo['blocks'].items():
        if blockinfo[1] != config['waves']['generatoraddress']:
            pass
        else:
            logger.debug(f"Block: {blockinfo[0]} mined!")
            # calculate waves rewards
            blockfees = previousblockinfo[2] * 0.6 + blockinfo[2] * 0.4
            leasersblockfees = (blockfees * int(config['waves']['percentagetodistribute']) / 100)
            leasersblockrewards = (blockrewards * int(config['waves']['percentagetodistribute']) / 100)
            nodeownerblockfees = blockfees - leasersblockfees
            nodeownerblockrewards = blockrewards - leasersblockrewards

            logger.debug(f"Block: {height}, leasers fees: {leasersblockfees}, leasers blockreward: {leasersblockrewards}")
            logger.debug(f"Block: {height}, nodeowner fees: {nodeownerblockfees}, nodeowner blockreward: {nodeownerblockrewards}")

            # find active leases for this block
            activeleasesatthisblock = getwavesactiveleasesatblock(height, leases_x_id)
            totalwavesshares = 0
            if len(activeleasesatthisblock['leases']) > 0:
                for address, lease_info in activeleasesatthisblock['leases'].items():
                    # initialize dictionary
                    if address not in payments:
                        payments[address] = {}
                        payments[address]['waves'] = {'id': 0, 'share': 0, 'reward': 0}
                        for token, details in config['waves']['airdrops'].items():
                            if details['enabled']:
                                if token not in payments[address]:
                                    payments[address][token] = {'id': details['assetid'], 'reward': 0}
                    # wavesshare for this lease
                    payments[address]['waves']['share'] = lease_info['total'] / activeleasesatthisblock['total']
                    logger.debug(f"Address: {address} leased: {lease_info['total']} total: {activeleasesatthisblock['total']} share: {payments[address]['waves']['share']}")

                    ############################################
                    # WAVES rewards
                    ############################################

                    fees = int(payments[address]['waves']['share'] * blockfees)
                    rewards = int(payments[address]['waves']['share'] * blockrewards)
                    payments[address]['waves']['reward'] += max(0, fees + rewards)
                    logger.debug(f"{address} fees: {fees}, rewards: {rewards}")

                    ############################################
                    # AIRDROPS rewards
                    ############################################

                    for token, details in config['waves']['airdrops'].items():
                        if details['enabled']:
                            payments[address][token]['reward'] += int(max(0, (payments[address]['waves']['share'] * leasersairdroprewards[token])))
                    
            # Node owner rewards
            nodeownerbeneficiaryaddress = config['waves']['nodeownerbeneficiaryaddress']
            if nodeownerbeneficiaryaddress not in payments:
                payments[nodeownerbeneficiaryaddress] = {}
                payments[nodeownerbeneficiaryaddress]['waves'] = {'id': 0, 'share': 0, 'reward': 0}
                for token, details in config['waves']['airdrops'].items():
                    if details['enabled']:
                        if token not in payments[nodeownerbeneficiaryaddress]:
                            payments[nodeownerbeneficiaryaddress][token] = {'id': details['assetid'], 'reward': 0}
            
            payments[nodeownerbeneficiaryaddress]['waves']['reward'] += int(max(0, nodeownerblockfees + nodeownerblockrewards))
            for token, details in config['waves']['airdrops'].items():
                if details['enabled']:
                    payments[nodeownerbeneficiaryaddress][token]['reward'] += int(max(0, nodeownerairdroprewards[token]))

        previousblockinfo = blockinfo

    return payments


def getleasesinfo(config, conn):
    leases_x_block = {}
    leases_x_id = {}

    try:
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM waves_leases")
        rows = cursor.fetchall()

        for row in rows:
            start = row[4]
            address = row[3]
            lease_id = row[1]

            if start not in leases_x_block:
                leases_x_block[start] = {}
            if address not in leases_x_block[start]:
                leases_x_block[start][address] = {}

            leases_x_block[start][address][lease_id] = row
            leases_x_id[lease_id] = row

        return leases_x_block, leases_x_id

    except sqlite3.Error as e:
        logger.info(f"SQLite error: {e}")
        return None, None


def loadblocksinfo(config, conn):
    global logger

    try:
        cursor = conn.cursor()

        # Get max height from waves_blocks
        cursor.execute("SELECT MAX(height) FROM waves_blocks")
        max_block_height = cursor.fetchone()[0]

        if max_block_height is None:
            logger.error("Error: waves_blocks table is empty.")
            return

        endblock = max_block_height - 1

        # Check if waves_payments has any rows
        cursor.execute("SELECT MAX(endblock) FROM waves_payments")
        max_endblock = cursor.fetchone()[0]

        if max_endblock is None:
            # waves_payments is empty, find min height from waves_blocks
            cursor.execute("SELECT MIN(height) FROM waves_blocks")
            startblock = cursor.fetchone()[0] - 1
        else:
            # waves_payments has rows, use max endblock
            startblock = max_endblock

        # endblock is always max block that is synced
        cursor.execute("SELECT MAX(height) FROM waves_blocks")
        endblock = cursor.fetchone()[0]

        # Load data from waves_blocks
        cursor.execute("SELECT * FROM waves_blocks WHERE height >= ? AND height <= ?", (startblock, endblock))
        blocks_data = cursor.fetchall()

        blocksinfo = {}
        blocksinfo['blocks'] = {}
        minedblocks = 0

        for row in blocks_data:
            height = row[0]
            blocksinfo['blocks'][height] = row
            if row[1] == config['waves']['generatoraddress']:
                minedblocks += 1

        blocksinfo['minedblocks'] = minedblocks
        blocksinfo['startblock'] = startblock
        blocksinfo['endblock'] = endblock

        return blocksinfo

    except sqlite3.Error as e:
        logger.error(f"SQLite error: {e}")


def main():
    global logger

    if len(sys.argv) != 2:
        logger.debug("Usage: python calculatepayments.py [dryrun Y!N]")
        sys.exit(1)

    logger = libs.setup_logger(log_file="l0ps.log", log_level=logging.DEBUG, name="calculatepayments")
    dryrun = sys.argv[1]
    config = libs.load_config_from_file('config.json')
    conn = sqlite3.connect(config['database'])  # Use the database filename from config
    pw.setNode(config['waves']['node'], config['waves']['chain'])
    addr = pw.address.Address(privateKey=config['waves']['pk'])
    logger.info("---------------------------------------")
    logger.info(f"Operating from address: {addr.address}")

    # Check if last payment had an error
    cursor = conn.cursor()
    cursor.execute("SELECT paymentlock FROM waves_payments ORDER BY id DESC LIMIT 1")
    result = cursor.fetchone()

    if result:  # If there is at least one payment
        payment_lock = result[0]
        if payment_lock == 'Y':
            logger.error("Error: Last payment is locked (paymentlock = 'Y').")
            cursor.close()
            conn.close()
            sys.exit(1)

    # Get node balances
    balances = libs.get_balances(config, addr)

    # Load info from blocks
    blocksinfo = loadblocksinfo(config, conn)

    logger.info(f"Start block: {blocksinfo['startblock']}")
    logger.info(f"End block: {blocksinfo['endblock']}")
    logger.info(f"Mined blocks: {blocksinfo['minedblocks']}")
    logger.info(f"Percentage distributed: {config['waves']['percentagetodistribute']}%");

    if blocksinfo['minedblocks'] == 0:
        logger.warning(f"No blocks were mined, exiting.")
        sys.exit(1)

    # Load leases info
    leases_x_block, leases_x_id = getleasesinfo(config, conn)

    # distribute payments
    payments = {}
    payments = distribute(config, blocksinfo, balances, leases_x_id)

    # compute fees and check node balance
    fees = 1  # 1 WAVES for safety
    totals = {}
    logger.debug("----- payments -----")
    for address, tokens in payments.items():
        if (address == config['waves']['nodeownerbeneficiaryaddress']):
            line = f"{address} (node owner),"
        else:
            line = f"{address},"
        for token, paymentdetails in tokens.items():
            if token in totals:
                totals[token] += int(paymentdetails['reward'])
            else:
                totals[token] = int(paymentdetails['reward'])
            if (token == 'waves'):
                line += f"share:{paymentdetails['share'] * 100:.5f}%,{token}:{paymentdetails['reward'] / 10 ** 8:.8f},"
            else:
                line += f"{token}:{paymentdetails['reward'] / 10 ** config['waves']['airdrops'][token]['decimals']:.8f},"
            fees += 0.001
        logger.debug(line)

    logger.debug("----- payments -----")
    fees = round(fees, 3) * 10 ** 8
    totalwavesneeded = int(totals['waves'] + fees)
    for token, amount in totals.items():
        if token == 'waves':
            logger.info(f"Total {token} to be sent: {amount / 10 ** 8:.8f}")
        else:
            logger.info(f"Total {token} to be sent: {amount / 10 ** config['waves']['airdrops'][token]['decimals']:.8f}")
    logger.info(f"Total fees: {fees / (10 ** 8)} WAVES")
    logger.info(f"Node Balance: {balances['waves']['balance'] / 10 ** 8} WAVES")
    logger.info(f"Total waves needed: {totalwavesneeded / 10 ** 8}")
    if (fees + totals['waves']) > balances['waves']['balance']:
        logger.info(f"Node debt: {(balances['waves']['balance'] - totalwavesneeded) / 10 ** 8}")
        exit("ERROR: Not enough balance: add waves to node balance, exiting.")
        logger.error("(Not enough balance: add waves to node balance, exiting.")

    savepayments(config, conn, payments, blocksinfo, totals, dryrun)
    logger.info("Calculated payments, you can now launch sendpayments.")

if __name__ == "__main__":
    main()
