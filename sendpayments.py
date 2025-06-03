import sys
import json
import requests
import pywaves as pw
import logging
import os
import libs
import sqlite3
import decimal

def pay(config, conn, addr, dryrun):

    global logger

    logger.info('Preparing recipient list...')

    try:

        # Find current payment

        sql = """
            SELECT max(id)
            FROM  waves_payments
            WHERE paymentlock = 'Y'
        """
        cursor = conn.cursor()
        cursor.execute(sql)
        rset = cursor.fetchone()
        

        currentpaymentid = rset[0]
        logger.info(f"Current payment: #{currentpaymentid}")

        if currentpaymentid == None:
            logger.warning(f"No payments to process has been found.")
            sys.exit(1)

        # find recipients for current payment
        sql = """
            SELECT
                pd.address,
                pd.amount,
                pd.token,
                pd.token_id
            FROM waves_paymentdetails pd
            WHERE pd.payment_id = ?
            AND pd.status = 'new';
            """

        cursor = conn.cursor()
        cursor.execute(sql, (currentpaymentid,))
        rset = cursor.fetchall()

        recipients = {}
        fees = 1
        totalpayments = 0

        for row in rset:
            address = row[0]
            amount = row[1]
            token = row[2]
            token_id = row[3]

            if token not in recipients:
                 recipients[token] = {'id': None, 'recipients': {}, 'payments': 0, 'total': 0}

            recipients[token]['token_id'] = token_id
            recipients[token]['total'] += amount
            recipients[token]['payments'] += 1
            recipients[token]['recipients'][address] = amount

            fees += 0.001
            totalpayments += 1

        fees = round(fees,3) * (10 ** 8)

        # check if there is enough balance

        balances = libs.get_balances(config, addr)
        logger.info(f"Total Payments: {totalpayments}")
        logger.info(f"Needed Fees: (safe) {int(fees)/(10**8)}")
        logger.info(f"Node Balance: {int(balances['waves']['balance']/(10**8))} $WAVES")

        if balances['waves']['balance'] < recipients['waves']['total'] + fees:
            logger.error(f"Not enough WAVES balance: {balances['waves']['balance']/(10**8):.8f} vs {(recipients['waves']['total']+fees)/(10**8):.8f}")
            return False

        for token, details in config['waves']['airdrops'].items():
            if details['enabled']:
                if balances[token]['balance'] < recipients[token]['total']:
                    logger.error(f"Not enough {token} balance: {balances[token]['balance']/(10**balances[token]['decimals']):.8f} vs {(recipients[token]['total']+fees)/(10**balances[token]['decimals']):.8f}")
                    return False

        # Pay

        for token, details in recipients.items():
            currentbatch = [] 
            logger.info("--------------------------------------")
            logger.info(f"Paying {token}...")
            for address, amount in details['recipients'].items():
                if len(currentbatch)<100:
                    if amount > 0:
                        currentbatch.append({ 'recipient': address, 'amount': amount })

            if len(currentbatch) == 100:
                masspay(config, token, details['token_id'], currentbatch, addr, dryrun)
                currentbatch = []
                time.sleep(1)

            if len(currentbatch) > 0:
                masspay(config, token, details['token_id'], currentbatch, addr, dryrun)

            # Saving data

            logger.info("Updating database...")

            sql = """
		UPDATE waves_paymentdetails 
                SET status = 'paid' 
                WHERE payment_id = ? 
                AND   status = 'new'
                AND   token = ?
            """
            cursor = conn.cursor()
            cursor.execute(sql, (currentpaymentid, token))

        # finally, update payment

        sql = """
            UPDATE waves_payments
            SET paymentlock = 'N' 
            WHERE id = ?
        """

        cursor = conn.cursor() 
        cursor.execute(sql, (currentpaymentid,))

        if (dryrun=='Y'):
            conn.rollback()
        else:
            conn.commit()

    except sqlite3.Error as e:
        logger.error(f"SQLLite error: {e}")
        conn.rollback()
        return False

    return True

def masspay(config, token, token_id, batch, addr, dryrun):

    logger.info('Number of payouts in batch: ' + str(len(batch)))
    #logger.info('paid from address: ' + addr.address)
    
    if token != 'waves':
        if (dryrun=='Y'):
            logger.info("Dryrun mode, not sending tx.")
            logger.debug('batch: ' + str(batch))
        else:
            pass
            tx = addr.massTransferAssets(batch, pw.Asset(token_id))
    else:
        if (dryrun=='Y'):
            logger.info("Dryrun mode, not sending tx.")
            logger.debug('batch: ' + str(batch))
        else:
           pass
           tx = addr.massTransferWaves(batch)

def main():


    if len(sys.argv) != 2:
        print("Usage: poetry run python sendpayments.py [dryrun: Y|N]")
        sys.exit(1)

    dryrun = sys.argv[1]

    global logger

    config = libs.load_config_from_file('config.json')
    logger = libs.setup_logger(log_file="l0ps.log", log_level=logging.DEBUG, name="sendpayments")
    conn = sqlite3.connect(config['database'])
    pw.setNode(config['waves']['node'], config['waves']['chain']);
    addr = pw.address.Address(privateKey=config['waves']['pk'])
    logger.info(f"Operating from address {addr.address}")

    rc = pay(config, conn, addr, dryrun)
    if (rc):
        logger.info("Payment has been succesfully completed.")
    else:
        logger.info("Some errors occurred while paying, check blockchain and update database accordingly.")

if __name__ == "__main__":
    main()
