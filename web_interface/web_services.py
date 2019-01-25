from modules_robot import _process_data_to_db


PG_PARAMS_VPN = {'dbname': 'testpython',
                     'host': '10.8.0.1',
                     'password': 'Oowead12',
                     'user': 'matt'}


def web_get_open_ordres(user_id):
    order_dict = {}
    db_conn = _process_data_to_db.Db_writer()
    db_request = db_conn.execute_request("SELECT id, market_name, open_price, \
        open_time, current_price, update_time, amount, user_id, timeframe, porog \
        from monitor where user_id = {};".format(user_id))
    db_conn.commit()
    db_conn.close()

    if db_request.rowcount > 0:
        for record in db_request:
            # print('record', record)
            order_id = record[0]
            currency = record[1]
            open_price = record[2]
            start_time = record[3]
            current_price = record[4]
            update_time = record[5]
            amount = record[6]
            user_id = record[7]
            timeframe = record[8]
            porog = record[8]
            order_dict[currency] = {
                'order_id': order_id,
                'market_name': currency,
                'open_price': open_price,
                'current_price': current_price,
                'open_time': start_time,
                'current_time': update_time,
                'status': 'open',
                'amount': amount,
                'user_id': user_id,
                'profit': 0,
                'srednee': 0,
                'counter': 0,
                'timeframe': timeframe,
                'porog': porog
            }
        # for currency in order_dict:
        #    print(currency, order_dict[currency])
        return order_dict