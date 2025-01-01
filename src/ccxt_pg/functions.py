import psycopg
from psycopg import sql

def prepare_items_for_pg(imported_items):
    """This function prepares CCXT order/trade items for an export to a PG database"""

    if type(imported_items) is not list:
        items = [imported_items]
    else:
        items = imported_items

    prepared_items = []

    for item in items:

        # Renaming order to order_id because of conflict in SQL
        if 'order' in item:
            item['order_id'] = item.pop('order')

        # Integrating empty statement in case of nonexistent values

        item['fee_cost'] = None
        item['fee_currency'] = None
        item['usdt_value'] = None
        item['asset_net_q'] = None

        if item['fee'] is not None:
            item['fee_cost'] = item['fee']['cost']
            item['fee_currency'] = item['fee']['currency']
        for fee in item['fees']:
            if float(fee['cost']) != 0:
                item['fee_cost'] = fee['cost']
                item['fee_currency'] = fee['currency']
        # item['exchange'] = client.name

        # Generate usdt_value column

        if item['fee_currency'] != 'USDT':
            item['usdt_value'] = item['cost']
        elif item['side'] == 'buy':
            item['usdt_value'] = item['cost'] + item['fee_cost']
        else:
            item['usdt_value'] = item['cost'] - item['fee_cost']

        # Generate asset_net_q columns

        if item['fee_currency'] != 'USDT':
            if item['fee_cost'] is not None:
                item['asset_net_q'] = item['amount'] - item['fee_cost']
            else:
                item['asset_net_q'] = None
        else:
            item['asset_net_q'] = item['amount']

        # Flatten dicts and lists in order to export them to columns.
        flattened_item = dict_to_text(item)
        prepared_items.append(flattened_item)

    return prepared_items

def dict_to_text(d):

    def convert(i_value):
        if isinstance(i_value, dict) or isinstance(i_value, list):
            return str(i_value)  # Convert sub-dict to string
        return i_value

    for key, value in d.items():
        d[key] = convert(value)
    return d


def retrieve_and_prepare_orders(client, ticker, start=None, end=None):

    """This function downloads all latest orders from a client."""

    # Bitget doesn't support fetch_closed_orders, hence the additional control flow complication.

    if client.name == 'Bitget':
        orders = client.fetch_canceled_and_closed_orders(symbol=ticker, limit=100, since=start, params={'until': end})
    else:
        orders = client.fetch_closed_orders(symbol=ticker, limit=100, since=start, params={'until': end})

    return prepare_items_for_pg(client, orders)


def retrieve_and_prepare_trades(client, pair, start=None, end=None):

    """This function downloads trades from a CCXT client and prepares them to export to a Postgres server."""

    trades = client.fetch_my_trades(symbol=pair, limit=100, since=start, params={'until': end})

    return prepare_items_for_pg(client, trades)

def export_to_sql(raw_ccxt_data, credentials, table):

    """Takes in a list of orders or trades in CCXT format, and a dict of PG credentials, and outputs the data to the attached DB."""

    dbname = credentials['POSTGRES_DB']
    user = credentials['POSTGRES_USER']
    password = credentials['POSTGRES_PASSWORD']
    host = credentials['POSTGRES_HOST']
    port = credentials['POSTGRES_PORT']

    data = prepare_items_for_pg(raw_ccxt_data)

    with psycopg.connect(f"dbname={dbname} user={user} password={password} host={host} port={port}") as conn:
        with conn.cursor() as cur:

            # Insert data
            columns = data[0].keys()  # Get the column names from the dictionary
            columns_identifiers = [sql.Identifier(col.lower()) for col in columns]

            # Insert query
            insert_query = sql.SQL("INSERT INTO {} ({}) VALUES ({})").format(
                sql.Identifier(table),
                sql.SQL(', ').join(columns_identifiers),
                sql.SQL(', ').join(sql.Placeholder() * len(columns))
            )

            # Convert dictionaries to tuple format for psycopg3
            values = [tuple(d.values()) for d in data]

            # Execute the insert for all rows
            cur.executemany(insert_query, values)

        print(f"Data inserted successfully in {table} table!")
