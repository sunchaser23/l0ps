import sys
import json
import requests
import pywaves as pw
import logging
import libs
import sqlite3
import datetime
from pprint import pprint
import traceback

INVOKE_FEE = 0.005

def savepayments(config, conn, payments, blocksinfo, totals, dryrun):
    global logger

    # insert payment entry
    now = datetime.datetime.now()
    sql = """
        INSERT INTO waves_payments (startblock, endblock, minedblocks, summary, paymentlock, timestamp) VALUES ( ?, ?, ?, ?, ?, ?)
        """
    try:
        cursor = conn.cursor()
        cursor.execute(sql, (blocksinfo['startblock']+1, blocksinfo['endblock'], blocksinfo['minedblocks'], json.dumps(totals), 'Y', now.isoformat()))
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
    activeleasesinfo = {
        'leases': {},
        'total': 0
    }
    # logger.debug(f"Block height: {height}")
    # Collect leases within the window of interest (last 1000 blocks)
    lower_bound = height - 1000
    grouped_by_address = {}

    for lease in leases_x_id.values():
        address = lease[3]
        start = lease[4]
        end = lease[7] if lease[7] is not None else float('inf')
        amount = lease[8]
        # logger.debug(f"lease: {lease}, address: {address}, start: {start}, end: {end}, amount: {amount}")
        # Check if lease fully cover [lower_bound, height]
        if start < lower_bound and height < end:

            if address not in activeleasesinfo['leases']:
                activeleasesinfo['leases'][address] = amount
            else:
                activeleasesinfo['leases'][address] += amount

            activeleasesinfo['total'] += amount
            continue

        # Check if lease intersects [lower_bound, height]
        if end < lower_bound or start > height:
            continue

        if address not in grouped_by_address:
            grouped_by_address[address] = []
        grouped_by_address[address].append((start, end, amount))

    for address, leases in grouped_by_address.items():
        # Build intervals from leases
        intervals = []
        min_amount = float('inf')

        for start, end, amount in leases:
            from_block = max(start, lower_bound)
            to_block = min(end, height)
            intervals.append((from_block, to_block))
            min_amount = min(min_amount, amount)

        # Sort intervals by start block
        intervals.sort()
        current = lower_bound
        fully_covered = True

        # Check leases by_address fully cover [lower_bound, height]
        for start, end in intervals:
            if start > current:
                fully_covered = False
                break
            current = max(current, end)

        if fully_covered and current >= height:
            if address not in activeleasesinfo['leases']:
                activeleasesinfo['leases'][address] = min_amount
            else:
                activeleasesinfo['leases'][address] += min_amount
            activeleasesinfo['total'] += min_amount

    # logger.debug(f"{activeleasesinfo}")

    return activeleasesinfo

def distribute(config, blocksinfo, balances, leases_x_id):
    airdroprewards = {}
    leasersairdroprewards = {}
    nodeownerairdroprewards = {}
    payments = {}
    enabledtokens = {}

    # Check if airdrop balances are sufficient before distribution
    # if not, exit with an error
    
    airdroppedtokens = {}
    for token, details in config['waves']['airdrops'].items():
        if details['enabled'] and token in balances:
            if balances[token]['balance'] < details['minamount']:
                logger.error(f"Error: {token} balance is less than {details['minamount']}, exiting.")
                sys.exit(1)
                
            airdroppedtokens[token] = {
                'id': details['assetid'],
                'decimals': details['decimals']
            }

    # Calculate airdrop rewards
    for token in airdroppedtokens:
        airdroprewards[token]=int(balances[token]['balance']/blocksinfo['minedblocks'])
        leasersairdroprewards[token]=airdroprewards[token]*(int(config['waves']['percentagetodistribute'])/100)
        nodeownerairdroprewards[token]=airdroprewards[token]-leasersairdroprewards[token]
    
    # calculate waves block rewards (fixed)
    previousblockinfo = blocksinfo['startblock']
    res = libs.blockchainrewards(config['waves']['node'])
    blockrewards = res['currentReward'] / 3
    
    for height, blockinfo in blocksinfo['blocks'].items():
        if blockinfo[1] != config['waves']['generatoraddress']:
            pass
        else:
            # calculate waves rewards
            blockfees = previousblockinfo[2] * 0.6 + blockinfo[2] * 0.4
            leasersblockfees = (blockfees * int(config['waves']['percentagetodistribute']) / 100)
            leasersblockrewards = (blockrewards * int(config['waves']['percentagetodistribute']) / 100)

            nodeownerblockfees = blockfees - leasersblockfees
            nodeownerblockrewards = blockrewards - leasersblockrewards
            
            # Leasers rewards

            activeleasesatthisblock = getwavesactiveleasesatblock(height, leases_x_id)
            totalwavesshares = 0
            if len(activeleasesatthisblock['leases']) > 0:
                for address, amountleased in activeleasesatthisblock['leases'].items():
                    # initialize dictionary
                    if address not in payments:
                        payments[address] = {}
                        payments[address]['waves'] = {'id': 0, 'share': 0, 'reward': 0}

                        # initialize airdrop rewards
                        for token, details in airdroppedtokens.items():
                            if token not in payments[address]:
                                payments[address][token] = {'id': details['id'], 'reward': 0}
                        
                    # wavesshare for this lease
                    payments[address]['waves']['share'] = amountleased / activeleasesatthisblock['total']

                    ############################################
                    # WAVES rewards
                    ############################################

                    fees = int(payments[address]['waves']['share'] * leasersblockfees)
                    rewards = int(payments[address]['waves']['share'] * leasersblockrewards)

                    payments[address]['waves']['reward'] += max(0, fees + rewards)
                    #logger.debug(f"{address} share: {payments[address]['waves']['share']}, fees: {fees}, rewards: {rewards}")

                    ############################################
                    # AIRDROPS rewards
                    ############################################

                    for token, details in airdroppedtokens.items():
                        if leasersairdroprewards[token] > 0:
                            payments[address][token]['reward'] += int(max(0, (payments[address]['waves']['share'] * leasersairdroprewards[token])))
                            #logger.debug(f"{address} {token} share: {payments[address]['waves']['share']}, airdrop reward: {leasersairdroprewards[token]}, total: {payments[address][token]['reward']}")
            # Node owner rewards
            
            nodeownerbeneficiaryaddress = config['waves']['nodeownerbeneficiaryaddress']
            if nodeownerbeneficiaryaddress not in payments:
                payments[nodeownerbeneficiaryaddress] = {}
                payments[nodeownerbeneficiaryaddress]['waves'] = {'id': 0, 'share': 0, 'reward': 0}
                for token, details in airdroppedtokens.items():
                    if token not in payments[nodeownerbeneficiaryaddress]:
                        payments[nodeownerbeneficiaryaddress][token] = {'id': details['id'], 'reward': 0}
            
            payments[nodeownerbeneficiaryaddress]['waves']['reward'] += int(max(0, nodeownerblockfees + nodeownerblockrewards))
            #logger.debug(f"{nodeownerbeneficiaryaddress} fees: {nodeownerblockfees}, rewards: {nodeownerblockrewards}, total: {nodeownerblockfees + nodeownerblockrewards}")
           
            for token, details in airdroppedtokens.items():
                payments[nodeownerbeneficiaryaddress][token]['reward'] += int(max(0, nodeownerairdroprewards[token]))
                #logger.debug(f"{nodeownerbeneficiaryaddress} {token} airdrop reward: {nodeownerairdroprewards[token]}, total: {payments[nodeownerbeneficiaryaddress][token]['reward']}")

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

        tx16calls = 0
        for row in blocks_data:
            height = row[0]
            blocksinfo['blocks'][height] = row
            if row[1] == config['waves']['generatoraddress']:
                minedblocks += 1
            tx16calls += row[6] 

        blocksinfo['minedblocks'] = minedblocks
        blocksinfo['startblock'] = startblock
        blocksinfo['endblock'] = endblock
        blocksinfo['tx16calls'] = tx16calls
        blocksinfo['nodetx16debt'] = tx16calls * INVOKE_FEE * 10 ** 8
        
        return blocksinfo

    except sqlite3.Error as e:
        logger.error(f"SQLite error: {e}")

def swap_calculate_readonly(config, asset_from, asset_to, amount):
    
    data = {
        "call": {
            "function": "swapCalculateREADONLY",
            "args": [
                {"type": "integer", "value": int(amount)},
                {"type": "string", "value": str(asset_from)},
                {"type": "string", "value": str(asset_to)}
            ]
        }
    }
    try:
                
        headers = {'Content-Type': 'application/json'}
   
        r = requests.post(config['waves']['node'] + 'utils/script/evaluate/' + config['swap']['wx_contract_address'], data=json.dumps(data), headers=headers)
        
        r.raise_for_status()
        result = r.json()     
        if 'result' in result:
            return result
        else:
            logger.error(f"Contract error: {json.dumps(result, indent=2)}")
            return None
    except Exception as e:
        logger.error(f"WX_CALC_ERROR: {traceback.format_exc()}")
        return None

def swap_execute(config, asset_from, asset_to, amount, amount_out_min):
    payment_asset = asset_from if asset_from != "WAVES" else None
    try:
        my_address = pw.address.Address(privateKey=config['waves']['pk'])
        tx = my_address.invokeScript(
            config['swap']['wx_contract_address'],
            'swap',
            [
                {"type": "integer", "value": amount_out_min},
                {"type": "string", "value": asset_to},
                {"type": "string", "value": config['waves']['generatoraddress']}
            ],
            [{"assetId": payment_asset, "amount": amount}]
        )
        if isinstance(tx, dict) and tx.get('error'):
            logger.error(f"WX swap error: {tx}")
            return None            
        pw.waitFor(tx['id'])
        return tx
    except Exception as e:        
        logging.error(f"WX_SWAP_ERROR: {traceback.format_exc()}")
        return None
    

def main():
    global logger

    if len(sys.argv) != 3:
        print("Usage: poetry run python calculatepayments.py [swapunit0 Y|N] [dryrun Y|N]")
        sys.exit(1)

    try:

        logger = libs.setup_logger(log_file="l0ps.log", log_level=logging.DEBUG, name="calculatepayments")
        swapunit0 = sys.argv[1]
        dryrun = sys.argv[2]
        
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
        logger.debug(f"Total tx16calls: {blocksinfo['tx16calls']}, debt: {blocksinfo['nodetx16debt']/10**8}")
        
        if blocksinfo['minedblocks'] == 0:
            logger.warning(f"No blocks were mined, exiting.")
            sys.exit(1)

        # swap unit0 to waves for canceling debt

        if swapunit0 == 'Y' and blocksinfo['nodetx16debt'] > 0:
            logger.info("Swapping Unit0 to WAVES")
            calc_unit0_price = swap_calculate_readonly(config,config['swap']['unit0_asset_id'], config['swap']['waves_asset_id'], 10**8)            
            unit0_price_inwaves = calc_unit0_price["result"]["value"]["_2"]["value"]
            logger.info(f"Unit0 price in waves: {unit0_price_inwaves/10**8}")                             
            logger.info(f"My Unit0 balance: {balances['unit0']['balance']/10**8}")
            unit0toswap = int(blocksinfo['nodetx16debt'] * 1.05/ unit0_price_inwaves * 10 ** 8)
            logger.info(f"Unit0 to swap: {unit0toswap/10**8}") 
            if dryrun == 'N':
                tx = swap_execute(config, config['swap']['unit0_asset_id'], config['swap']['waves_asset_id'], int(unit0toswap), int(blocksinfo['nodetx16debt']))            
                if (tx is not None):
                    logger.info("Unit0 swapped to WAVES")
                else:
                    logger.error("Error: Unit0 swap failed, exiting.")
                    sys.exit(1)
                # refresh balances
                balances = libs.get_balances(config, addr)
        else:
            logger.info("Not swapping Unit0 to WAVES")
        

        # Load leases info
        leases_x_block, leases_x_id = getleasesinfo(config, conn)

        # distribute payments
        payments = {}
        payments = distribute(config, blocksinfo, balances, leases_x_id)
        
        # foreach payments, remove entries with amount 0 and removesending fees
        for address, tokens in list(payments.items()):
            for token, paymentdetails in list(tokens.items()):
                if paymentdetails['reward'] <= 0:
                    del tokens[token]
            if not tokens:
                del payments[address]

        for address, tokens in payments.items():
            if 'waves' in tokens:
                n = len(tokens)
                tokens['waves']['reward'] = max(0, tokens['waves']['reward'] - (0.001 * 10 ** 8 * n))

        # check node balance vs amount to be sent
        totals = {}
        
        logger.debug("-------------------- Payments --------------------")
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
                    line += f"{token}:{paymentdetails['reward'] / 10 ** 8:.8f},share:{paymentdetails['share'] * 100:.2f}%,"
                else:
                    line += f"{token}:{paymentdetails['reward'] / 10 ** config['waves']['airdrops'][token]['decimals']:.8f},"
            logger.debug(line)
        logger.debug("--------------------------------")

        totalwavesneeded = int(totals['waves'])
        for token, amount in totals.items():
            if token == 'waves':
                logger.info(f"Total {token} to be sent: {amount / 10 ** 8:.8f}")
            else:
                logger.info(f"Total {token} to be sent: {amount / 10 ** config['waves']['airdrops'][token]['decimals']:.8f}")

        logger.info(f"Node Balance: {balances['waves']['balance'] / 10 ** 8} WAVES")
        logger.info(f"Total waves needed: {totalwavesneeded / 10 ** 8}")
        
        if (totals['waves']) > balances['waves']['balance']:
            logger.info(f"Node debt: {(balances['waves']['balance'] - totalwavesneeded) / 10 ** 8}")
            logger.error("Not enough balance: add waves to node balance, exiting.")
            #sys.exit(1)
            
        savepayments(config, conn, payments, blocksinfo, totals, dryrun)
        if (dryrun == 'N'):
            logger.info("Calculated payments, you can now launch sendpayments.")
        else:
            logger.info("Calculated payments, no payments were saved.")
            
    except Exception as e:
        logger.error(f"Error: {e}")
        logger.error(traceback.format_exc())
        sys.exit(1)

if __name__ == "__main__":
    main()
