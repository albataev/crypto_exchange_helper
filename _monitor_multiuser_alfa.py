import algorithms
import time
from modules_robot import _process_data_to_db

#LOCAL==================
PG_PARAMS = {'dbname': 'postgres',
                     'host': 'localhost',
                     'password': '',
                     'user': 'postgres'}

API_V1_1 = 'v1.1'


def query_db_for_open_orders(db_writer_obj):
    '''
    :param db_writer_obj:
    methods:
    execute_request - same as cursor execute
    commit - commit request
    :return: Dictionary with open orders for each currency
    :rtype : dict
    Example ::
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
            'porog': porog, # stop loss %
            'ma_small_value': ma_small_value,
            'ma_medium_value': ma_medium_value,
            'ma_porog_srabativaniya': ma_porog_srabativaniya
            }
    '''
    order_dict = {}
    #porog - процентный порог высчитывания стоплосса
    # db_request = db_writer_obj.execute_request("SELECT id, market_name, open_price, \
    # open_time, current_price, update_time, amount, user_id, timeframe, porog, \
    # ma_small_value, ma_medium_value, ma_porog_srabativaniya FROM monitor WHERE status = 'open';")
    db_request = db_writer_obj.execute_request("SELECT id, market_name, open_price, \
    open_time, current_price, update_time, amount, user_id, timeframe, porog, \
    ma_small_value, ma_medium_value, ma_porog_srabativaniya FROM monitor WHERE status = 'open';")
    db_writer_obj.commit()
    if db_request.rowcount > 0:
        for record in db_request:
            #print('record', record)
            order_id = record[0]
            currency = record[1]
            open_price = record[2]
            start_time = record[3]
            current_price = record[4]
            update_time = record[5]
            amount = record[6]
            user_id = record[7]
            timeframe = record[8]
            porog = record[9]
            ma_small_value = record[10]
            ma_medium_value = record[11]
            ma_porog_srabativaniya = record[12]
            if currency not in order_dict:
                order_dict[currency] = {}
            order_dict[currency][order_id] = {
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
                'porog': porog, # stop loss %
                'ma_small_value': ma_small_value,
                'ma_medium_value': ma_medium_value,
                'ma_porog_srabativaniya': ma_porog_srabativaniya
            }
        #for currency in order_dict:
        #    print(currency, order_dict[currency])
        return order_dict
    else:
        return None


# def get_updated_rates_from_db(market_name, last_select_time):
#     db_updated_data_request = "SELECT marketname, last, query_tstamp from crypto where marketname = '{}' AND query_tstamp > '{}' ORDER BY id ASC;".format \
#         (market_name, last_select_time)
#     return DB_CONNECTOR.execute_request(db_updated_data_request) #нужен ли commit???


# def close_order(order):
#     # sql_completed_trades_query = "INSERT INTO completed_trades(\
#     # market_name, open_price, open_time,\
#     # close_price, close_time, profit, amount, user_id, comment) \
#     # VALUES ('{}', {}, '{}', {}, '{}', {}, '{}', {}, {});".format( \
#     #     order.market_name, order.open_price, order.open_time, \
#     #     order.current_price, order.update_time, order.profit, order.amount, order.user_id, order.status_comment)
#     # DB_CONNECTOR.execute_request(sql_completed_trades_query)
#     # print('Order with id', order.order_id, 'closed')
#     # #========mеняем статус ордера на closed в базе данных monitor
#     print('IN FUNCTION: close_order:\n\n')
#     sql_change_monitor_to_closed = "UPDATE monitor SET status = 'closed', close_price = {}, profit = {} WHERE id = '{}';".format( \
#         order.current_price, order.profit, order.order_id)
#     print('IN FUNCTION: close_order After sql prepared:\n\n')
#     print('+++++++++++++++++++sql_update_monitor_order: ', sql_change_monitor_to_closed)
#     DB_CONNECTOR.execute_request(sql_change_monitor_to_closed)
#     DB_CONNECTOR.commit()


def update_open_order(order):
    #print('ORDER: ', order)
    #print('ORDER profit: ', order.profit, 'Order ID: ', order.order_id)
    #print('IN FUNCTION: update_open_order\n\n')
    #print('+++++++++++++++++++UPDATING ORDER IN DB+++++++++++++++++++')
    sql_update_monitor_order = "UPDATE monitor SET update_time = '{}', current_price = {}, profit = {}, status = '{}', sl = {}, comment = '{}', algo = '{}' WHERE id = '{}';".format( \
        order.update_time, order.current_price, order.profit, order.status, order.stop_loss, order.status_comment, order.algo, order.order_id)
    #print('IN FUNCTION: update_open_order after SQL reauest prepared\n\n')
    print('+++++++++++++++++++sql_update_monitor_order: ', sql_update_monitor_order)
    DB_CONNECTOR.execute_request(sql_update_monitor_order)
    DB_CONNECTOR.commit()


def order_monitor(db_connection):
    #возможно имеет смысл брать усреднение по кратности интервала сигнала.
    #то есть если сигнал был по 5 минутному, то усредняем 3 * 5 минут, но - риск потерь выше
    algo_dict = {}
    # ====SETTING INITIAL VALUES=========
    while True:
        open_orders_from_db = query_db_for_open_orders(db_connection)
        if open_orders_from_db is not None:
            for market in open_orders_from_db: #итерируем по рынку
                if market not in algo_dict:
                    algo_dict[market] = {}
                for order_id in open_orders_from_db[market]:
                    if order_id not in algo_dict[market]:
                        print('Checking key order ID ', order_id, open_orders_from_db[market])
                        algo_dict[market][order_id] = algorithms.Algo(db_connection, open_orders_from_db[market][order_id])
                        continue # не надо продолжать цикл при инициализации, т.к. получены все свежие данные
                    updated_rates = algorithms.get_updated_rates_from_db(db_connection, market, algo_dict[market][order_id].update_time)
                    if updated_rates.rowcount > 0: #если получен ответ от БД, содержащий данные
                        print('Processing market: ', market)
                        algo_dict[market][order_id].average_per_interval(updated_rates.fetchall())
                        update_open_order(algo_dict[market][order_id])
                        # присваиваем статус закрыт внутри класса, и здесь просто его удаляем
                        if (algo_dict[market][order_id].status == 'closed') or \
                           (algo_dict[market][order_id].status == 'ERROR'):
#                            print(algo_dict[market][order_id].item)
                            # ====ЗАКРЫВАЕМ ОРДЕР====================
                            #close_order(algo_dict[market][order_id])
                            # удаляем объект
                            del(algo_dict[market][order_id])
                            #break
            print('Database - no fresh data')
            time.sleep(4)
        else:
            print('no items with open status')
            time.sleep(7)

if __name__ == '__main__':
    DB_CONNECTOR = _process_data_to_db.Db_writer(PG_PARAMS)
    if DB_CONNECTOR is not None:
        while True:
            try:
                qqq = order_monitor(DB_CONNECTOR)

            except KeyboardInterrupt:
                print('keyboard interrupt, DB conection closed')
                DB_CONNECTOR.close()
                break
    else:
        print('DB adapter failure')




