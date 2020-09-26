"""
Example of usage NEAR Indexer for Explorer database to get the original Transaction
of the TRANSFER Receipt for specific block.

It searches for Receipt Action with ``action_kind`` == 'TRANSFER' and then finds the
Transaction the Receipt belongs to and returns the Transaction

Usage:
   ```
   python transfer.py <block_height>
   ```

Getting started:

You need to have python 3.8.5 version.

    ```
    pip install -r requirements.txt
    ```

Create `.env` file near with connection string to database

    ```# .env
    DATABASE_URI=postgres://user:password@host/database_name?ssl=True
    ```

Alternatively you can pass this as env var before running the script.

    ```
    DATABASE_URI="postgres://user:password@host/database_name?ssl=True" python transfer.py <block_height>
    ```
"""

import sys
import os

from pprint import pprint

from tortoise import Tortoise, run_async
from dotenv import load_dotenv


async def init():
    """
    Load env variable from .env file and use DATABASE_URI to establish connection to database
    """
    load_dotenv()
    try:
        database_uri = os.environ['DATABASE_URI']
    except KeyError:
        raise ValueError("DATABASE_URI env var had to be provided.")
    await Tortoise.init(
        db_url=database_uri,
        modules={'models': []}
    )

    await Tortoise.generate_schemas()


async def fetch_transactions_for_transfer_receipt_by_block_height(block_height):
    """
    Performs search for receipt_action with action_kind of TRANSFER (excluding those where predecessor_id = 'system')
    Find corresponding transactions and return a list.
    :param block_height: int
    :return: list of dicts with transactions, or empty list if nothing found
    """
    conn = Tortoise.get_connection("default")

    _, response = await conn.execute_query(
        f"""
            SELECT (
                transactions.*
            ) FROM receipt_action_actions
            INNER JOIN receipts ON receipt_action_actions.receipt_id = receipts.receipt_id
            INNER JOIN execution_outcomes ON execution_outcomes.receipt_id = receipts.receipt_id
            INNER JOIN transactions ON transactions.transaction_hash = receipts.transaction_hash
            WHERE receipt_action_actions.action_kind = 'TRANSFER' 
                AND receipts.predecessor_id != 'system' 
                AND receipts.block_height = {block_height}
        """
    )

    # TODO: Consider to optimize separate queries
    transactions = []
    for tx in response:
        transaction = dict(tx)

        _, tx_actions = await conn.execute_query(
            f"""
            SELECT * FROM transaction_actions
            WHERE transaction_hash = '{transaction['transaction_hash']}'
            """
        )

        transaction['actions'] = [dict(action) for action in tx_actions]

        receipts = []
        _, tx_receipts = await conn.execute_query(
            f"""
            SELECT * FROM receipts
            JOIN receipt_actions ON receipt_actions.receipt_id = receipts.receipt_id
            JOIN execution_outcomes ON execution_outcomes.receipt_id = receipts.receipt_id
            WHERE receipts.transaction_hash = '{transaction['transaction_hash']}'
            """
        )
        for rec in tx_receipts:
            receipt = dict(rec)

            _, r_actions = await conn.execute_query(
                f"""SELECT * FROM receipt_action_actions WHERE receipt_id = '{receipt['receipt_id']}' ORDER BY index ASC"""
            )
            receipt['actions'] = [dict(action) for action in r_actions]
            receipts.append(receipt)

        transaction['receipts'] = receipts
        transactions.append({'transaction': transaction})

    return transactions


async def run():
    try:
        block_height = sys.argv[1]
    except KeyError:
        raise ValueError("Block height is not provided. HINT: python transfer.py 10000001")

    await init()

    pprint(await fetch_transactions_for_transfer_receipt_by_block_height(block_height))


if __name__ == '__main__':
    run_async(run())
