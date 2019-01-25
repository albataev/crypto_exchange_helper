from datetime import datetime
import time
from modules_robot import _process_data_to_db, bittrex
import argparse


#VPN==================
PG_PARAMS = {'dbname': 'postgres',
                     'host': 'localhost',
                     'password': '',
                     'user': 'postgres'}

API_V1_1 = 'v1.1'
#USER_ID = 1

def make_db_conn(conn_params):
    try:
        db_conn = _process_data_to_db.Db_writer(conn_params)
        return db_conn
    except Exception as error:
        print('Error conectiong to DB', error)
        return None

DB_CONNECTION = make_db_conn(PG_PARAMS)

def prepare_sql_completed_buy_order_query(order_data_dictionary, timeframe, porog,
                                          ma_small_value, ma_medium_value,
                                          ma_porog_srabativaniya):
    # принимает на вход данные по исполненному ордеру и формирует готовый SQL запрос для помещения
    # валюты и ее количества в БД monitor
    buy_order = order_data_dictionary
    print('order_data_dictionary', order_data_dictionary)
    order_dict = {
        'market_name': buy_order['order_details']['result']['Exchange'],
        'open_price': buy_order['order_details']['result']['PricePerUnit'],
        'current_price': buy_order['order_details']['result']['PricePerUnit'],
        'open_time': buy_order['open_time'],
        'current_time': buy_order['open_time'],
        'status': 'open',
        'amount': buy_order['order_details']['result']['Quantity'],
        'btc_amount': buy_order['btc_amount'],
        'profit': 0,
        'user_id': buy_order['user_id'],
        'porog': porog,
        'timeframe': timeframe,
        'sl': 0,
        'ma_small_value': ma_small_value,
        'ma_medium_value': ma_medium_value,
        'ma_porog_srabativaniya': ma_porog_srabativaniya
    }
    sql_completed_order = \
        "INSERT INTO monitor(market_name, open_price, \
        open_time, current_price, update_time, amount, user_id, status, porog, timeframe, profit, sl, btc_amount, \
        ma_small_value, ma_medium_value, ma_porog_srabativaniya) \
        VALUES (\'{market_name}\', {open_price}, '{open_time}', {current_price}, " \
        "'{update_time}', {amount}, '{user_id}', '{status}', {porog}, " \
        "{timeframe}, {profit}, {sl}, {btc_amount}, {ma_small_value}, " \
        "{ma_medium_value}, {ma_porog_srabativaniya});".format( \
        market_name=order_dict['market_name'],
        open_price=order_dict['open_price'],
        open_time=order_dict['open_time'],
        current_price=order_dict['current_price'],
        update_time=order_dict['current_time'],
        amount=order_dict['amount'],
        user_id=order_dict['user_id'],
        status=order_dict['status'],
        porog=order_dict['porog'],
        timeframe=order_dict['timeframe'],
        profit=order_dict['profit'],
        sl=order_dict['sl'],
        btc_amount=order_dict['btc_amount'],
        ma_small_value=order_dict['ma_small_value'],
        ma_medium_value=order_dict['ma_medium_value'],
        ma_porog_srabativaniya=order_dict['ma_porog_srabativaniya'])
    print(sql_completed_order)
    return sql_completed_order


def get_user_data(db_connector, user_id):
    #!!!!!!!!!!!проверить если нигде более не используется, объединить с create_personalized_bittrex_connection
    # возвращает api_key и api_secret для конкретного user_id
    sql_request = "SELECT api_key, api_secret FROM users WHERE id = {};".format(user_id)
    user_data = db_connector.execute_request(sql_request).fetchone()
    db_connector.commit()
    return user_data


def create_personalized_bittrex_connection(db_connector, user_id):
    #принимает данные для подключения к битриксу конкретного пользователя
    #возвращает класс соединения с битриксом
    user_data = get_user_data(db_connector, user_id)
    print('USER_DATA FROM DB', user_data)
    user_api_key = user_data[0]
    user_api_secret = user_data[1]
    bittrex_conn = bittrex.Bittrex(user_api_key, user_api_secret, api_version=API_V1_1)
    return bittrex_conn


def query_db_for_sell_all(user_id):
    order_dict = {}
    db_request = DB_CONNECTION.execute_request("SELECT id, market_name, \
    amount, user_id from monitor where status = 'open' AND user_id = '{}';".format(user_id))
    DB_CONNECTION.commit()
    if db_request.rowcount > 0:
        for record in db_request:
            #print('record', record)
            order_id = record[0]
            currency = record[1]
            amount = record[2]
            user_id = record[3]
            if currency not in order_dict:
                order_dict[currency] = {}
            order_dict[currency][order_id] = {
                'order_id': order_id,
                'market_name': currency,
                'amount': amount,
                'user_id': user_id
            }
        #for currency in order_dict:
            #print(currency, order_dict[currency])
        return order_dict
    else:
        return None


def sell_all_close_order(order_id):  # сделал для веб версии костыль. Вообще он в мониторе живет
    # ========mеняем статус ордера на closed в базе данных monitor
    sql_change_monitor_to_closed = "UPDATE monitor SET status = 'closed' WHERE id = '{}';".format(int(order_id))
    DB_CONNECTION.execute_request(sql_change_monitor_to_closed)
    DB_CONNECTION.commit()


def sell_all(bittrex_connector, user_id):
    orders_for_sell = query_db_for_sell_all(user_id)
    if orders_for_sell is not None:
        for market in orders_for_sell:
            for id in orders_for_sell[market]:
                print(market, orders_for_sell[market][id]['user_id'], \
                      orders_for_sell[market][id]['order_id'], orders_for_sell[market][id]['amount'])
                smart_sell(bittrex_connector, orders_for_sell[market][id]['user_id'], \
                           market, orders_for_sell[market][id]['amount'])
                sell_all_close_order(orders_for_sell[market][id]['order_id'])

def smart_sell(bittrex_connector, user_id, market_name, amount = None, multiplyer_val = 0.001, sell_all = True):
    price_coeff = 1
    result = {
        'order_details': {
            'result': {},
            'message': ''
        }
    }
    order_filled = False
    stakan_attempts = 0
    balance_attempts = 0
    success_attempts = 0
    sell_attempts = 0
    amount_to_sell = amount
    #===Проверяем баланс - есть ли средства на покупку=====
    #
    #
    while True:
        try:
            print('Получаем баланс, попытка ', balance_attempts)
            balance = bittrex_connector.get_balance(market_name.split('-')[1])
            print('Balance: ', balance)
            if balance is not None and balance['success']:
                if balance['result']['Available'] > 0:
                    if (amount is None) or (amount > balance['result']['Available']):
                        amount_to_sell = balance['result']['Balance']
                    break
                else:
                    result['order_details']['error'] = 'No money'
                    result['order_details']['success'] = False
                    return result
            elif balance is not None and not balance['success']:
                print('Ошибка при получении баланса. Ответ False: ', balance)
                result['order_details']['success'] = False
                result['order_details']['error'] = balance
                return result
            else:
                print('Ошибка при получении баланса. Ответ False: ', balance)
                result['order_details']['success'] = False
                result['order_details']['error'] = balance
                return result
        except Exception as e:
            print('Невозможно получить баланс', e)
            result['order_details']['success'] = False
            result['order_details']['error'] = e
            if balance_attempts == 3:
                print('Невозможно получить баланс за 3 попытки')
                result['order_details']['success'] = False
                result['order_details']['error'] = '3 attempts made, no result'
                return result
        balance_attempts += 1
        time.sleep(0.5)
    #
    #
    #END ===Проверяем баланс - есть ли средства на покупку=====

    #===Получаем цену валюты=====
    #
    #
    # покупаем по стакану depth_type='buy'
    # чтобы гарантированно купить берем 10-ю цену?
    while True:
        try:
            print('Получаем стакан, попытка ', stakan_attempts)
            stakan = bittrex_connector.get_orderbook(market_name, depth_type='buy')
            if stakan is not None and stakan['success']:
                break
            elif stakan is not None and not stakan['success']:
                print('Ошибка при получении стакана. Ответ False: ', stakan)
                result['order_details']['error'] = stakan
                result['order_details']['success'] = False
                return result
        except Exception as e:
            print('Невозможно получить стакан', e)
            result['order_details']['error'] = e
            result['order_details']['success'] = False
            return result
        if stakan_attempts == 3:
            print('Невозможно получить стакан за 3 попытки')
            result['order_details']['error'] = '3 attempts made'
            result['order_details']['success'] = False
            return result
        stakan_attempts += 1
        time.sleep(0.5)
    #берем цену на 0.15% выше самой дешевой, чтобы гарантированно купить
    current_price = 0
    for index, item in enumerate(stakan['result']):
        if item['Rate'] > stakan['result'][0]['Rate'] * 0.998:
            current_price = item['Rate']
            print('Номер цены в стакане: ', index, 'Цена: ',current_price)
            break
    if current_price == 0: # не смогли найти ордер с ценой дороже на 0.15%
        print('Недостаточно цен в стакане для получения требуемой цены, начинаем покупать от верхней')
        current_price = stakan['result'][0]['Rate']

    #
    #
    #END===Получаем цену валюты=====

    print('Selling with USER_ID: ', user_id)
    print('Amount to sell: ', amount_to_sell)
    #
    #
    #ПРОДАЕМ
    while not order_filled:
        order = bittrex_connector.sell_limit(market_name, amount_to_sell, current_price)
        try:
            print('Продаем, попытка ', sell_attempts)
            while True:
                print('Selling ', amount_to_sell, market_name.split('-')[1], 'with price: ', current_price,
                      'price_coeff: ', price_coeff)
                print(market_name, current_price, amount_to_sell)
                print('SELL limit order: ', order)
                time.sleep(0.3)
                if order is not None and order['success']:
                    #здесь выполняем все проверки
                    #
                    #
                    uuid = order['result']['uuid']
                    print('Selling with price: ', current_price, 'price_coeff: ', price_coeff)
                    time.sleep(0.2)
                    status_ispolneniya = bittrex_connector.get_order(uuid)
                    if status_ispolneniya is not None and not status_ispolneniya['result']['IsOpen']: #ордер закрыт. Значит выполнен
                        print('Ордер выполнен без ОТМЕНЫ: ', status_ispolneniya)
                        order_filled = True
                        break
                    elif status_ispolneniya is not None and status_ispolneniya['result']['IsOpen']:  # ордер не закрыт, надо отменить и создать с повышенной ценой
                        # если ордер выполнен частично - берем остаток из него
                        cancel_result = bittrex_connector.cancel(uuid)
                        time.sleep(0.5)
                        status_ispolneniya = bittrex_connector.get_order(uuid)
                        if cancel_result is not None and not cancel_result['success']:  # Не получилось ОТМЕНИТЬ ордер - значит он уже закрыт
                            #хуй там, ордер может остаться открытым! проверяем заново:
                            if status_ispolneniya is not None and not status_ispolneniya['result']['IsOpen']:
                                print('CANCEL отмена НЕ выполнена, ордер закрыт, статус проверен: ', cancel_result)
                                # обновляем статус и выходим
                                order_filled = True
                                break
                        elif cancel_result is not None and cancel_result['success']:  # Ордер отменен. Покупаем заново
                            print('CANCEL отмена ВЫПОЛНЕНА: ', cancel_result)
                            print('Double check: ')
                            status_ispolneniya = bittrex_connector.get_order(uuid)
                            if status_ispolneniya['result']['IsOpen'] or \
                               status_ispolneniya['result']['QuantityRemaining'] > 0:  # ордер не выполнен
                                cancel_result = bittrex_connector.cancel(uuid) #повторно отменяем
                                price_coeff -= multiplyer_val
                                current_price = current_price * price_coeff
                                #СЮДА НАДО ДОБАВИТЬ ПОЛУЧЕНИЕ ИЗ СТАКАНА!!!
                                amount_to_sell = status_ispolneniya['result']['QuantityRemaining']
                                print('Новая цена продажи: ', current_price)
                                print('Остаток к продаже: ', amount_to_sell)
                            else:  # ордер исполнился с момента выполнения отмены. Так бывает. И исполнен целиком
                                print('ОРДЕР ВЫПОЛНЕН НЕ СМОТРЯ НА ОТМЕНУ: ', status_ispolneniya)
                                order_filled = True
                                break #прерывание внутреннего цикла по процессингу ордера
                elif order is not None and not order['success']:
                    if success_attempts == 3:
                        print('Ошибка при выполнении продажи. Ответ False: ', order)
                        result['order_details']['error'] = order
                        result['order_details']['message'] = order['message']
                        result['order_details']['success'] = False
                        return result
                    success_attempts += 1
                    time.sleep(0.5)
                else:
                    if sell_attempts == 3:
                        print('Невозможно выполнить продажу за 3 попытки')
                        result['order_details']['error'] = '3 attempts made'
                        result['order_details']['success'] = False
                        result['order_details']['message'] = 'Not sold for 3 attempts'
                        return result
                    sell_attempts += 1
                    time.sleep(0.5)
            if market_name.split('-')[0].upper() == 'USDT':
                usdt_btc_market_data = bittrex_connector.get_ticker('USDT-BTC')
                btc_amount = status_ispolneniya['result']['Price'] / usdt_btc_market_data['result']['Ask']
            elif market_name.split('-')[0].upper() == 'BTC':
                btc_amount = status_ispolneniya['result']['Price']
            result['user_id'] = user_id
            result['uuid'] = uuid
            result['btc_amount'] = btc_amount
            result['order_details']['success'] = False
            result['order_details'] = status_ispolneniya
            result['open_time'] = datetime.fromtimestamp(time.time()).strftime("%Y-%m-%d %H:%M:%S.%f")
        except Exception as e:
            print('Невозможно выполнить покупку', e)
            result['order_details']['error'] = e
            result['order_details']['success'] = False
    return result






















def smart_buy(bittrex_connector, market_name, base_currency_amount, user_id, multiplyer_val = 0.0015):
    price_coeff = 1
    result = {
        'order_details': {
            'result': {}
        }
    }
    order_filled = False
    stakan_attempts = 0
    balance_attempts = 0
    buy_attempts = 0
    #===Проверяем баланс - есть ли средства на покупку=====
    #
    #
    while True:
        print("In true selling - getting balance")
        try:
            print('Получаем баланс, попытка ', balance_attempts)
            balance = bittrex_connector.get_balance(market_name.split('-')[0])
            print('SmartBuy balance: ', balance)
            if balance is not None and balance['success']:
                if balance['result']['Available'] < base_currency_amount:
                    result['order_details']['success'] = False
                    result['order_details']['error'] = 'На балансе недостаточно средств'
                    result['order_details']['result']['error'] = 'На балансе недостаточно средств'
                    return result
                else:
                    break
            elif balance is not None and not balance['success']:
                print('Ошибка при получении баланса. Ответ False: ', balance)
                result['order_details']['success'] = False
                result['order_details']['error'] = balance
                return result
            else: # баланс получен успешно, денег на счету достаточно
                break
        except Exception as e:
            print('Невозможно получить баланс', e)
            result['order_details']['success'] = False
            result['order_details']['error'] = e
            if balance_attempts == 3:
                print('Невозможно получить баланс за 3 попытки')
                result['order_details']['success'] = False
                result['order_details']['error'] = '3 attempts made, no result'
                return result
        balance_attempts += 1
        time.sleep(0.5)
    #
    #
    #END ===Проверяем баланс - есть ли средства на покупку=====

    #===Получаем цену валюты=====
    #
    #
    # покупаем по стакану depth_type='sell'
    # чтобы гарантированно купить берем 10-ю цену?
    while True:
        try:
            print('Получаем стакан, попытка ', stakan_attempts)
            stakan = bittrex_connector.get_orderbook(market_name, depth_type='sell')
            if stakan is not None and stakan['success']:
                break
            elif stakan is not None and not stakan['success']:
                print('Ошибка при получении стакана. Ответ False: ', stakan)
                result['order_details']['error'] = stakan
                result['order_details']['success'] = False
                return result
        except Exception as e:
            print('Невозможно получить стакан', e)
            result['order_details']['error'] = e
            result['order_details']['success'] = False
            return result
        if stakan_attempts == 3:
            print('Невозможно получить стакан за 3 попытки')
            result['order_details']['error'] = '3 attempts made'
            result['order_details']['success'] = False
            return result
        stakan_attempts += 1
        time.sleep(0.5)
    #берем цену на 0.15% выше самой дешевой, чтобы гарантированно купить
    current_price = 0
    for index, item in enumerate(stakan['result']):
        if item['Rate'] > stakan['result'][0]['Rate'] * 1.002:
            current_price = item['Rate']
            print('Номер цены в стакане: ', index, 'Цена: ',current_price)
            break
    if current_price == 0: # не смогли найти ордер с ценой дороже на 0.15%
        print('Недостаточно цен в стакане для получения требуемой цены, начинаем покупать от верхней')
        current_price = stakan['result'][0]['Rate']
    sum_to_buy = base_currency_amount / current_price
    #
    #
    #END===Получаем цену валюты=====

    print('Byuing with USER_ID: ', user_id)
    print('Amount to buy: ', sum_to_buy)
    #
    #
    #ПОКУПАЕМ
    while not order_filled:
        try:
            print('Покупаем, попытка ', buy_attempts)
            order_v_processe_pokupki = bittrex_connector.buy_limit(market_name, sum_to_buy, current_price)
            while True:
                if order_v_processe_pokupki is not None and order_v_processe_pokupki['success']:
                    #здесь выполняем все проверки
                    #
                    #
                    uuid = order_v_processe_pokupki['result']['uuid']
                    print('Byuing with price: ', current_price, 'price_coeff: ', price_coeff)
                    time.sleep(0.2)
                    status_ispolneniya = bittrex_connector.get_order(uuid)
                    if status_ispolneniya is not None and not status_ispolneniya['result']['IsOpen']: #ордер закрыт. Значит выполнен
                        print('Ордер выполнен без ОТМЕНЫ: ', status_ispolneniya)
                        order_filled = True
                        break
                    elif status_ispolneniya is not None and status_ispolneniya['result']['IsOpen']:  # ордер не закрыт, надо отменить и создать с повышенной ценой
                        # если ордер выполнен частично - берем остаток из него
                        cancel_result = bittrex_connector.cancel(uuid)
                        if cancel_result is not None and not cancel_result['success']:  # Не получилось ОТМЕНИТЬ ордер - значит он уже закрыт
                            #хуй там, ордер может остаться открытым! проверяем заново:
                            time.sleep(0.5)
                            status_ispolneniya = bittrex_connector.get_order(uuid)
                            if status_ispolneniya is not None and status_ispolneniya['result']['IsOpen']:
                                time.sleep(0.5)
                                bittrex_connector.cancel(uuid)
                                time.sleep(0.5)
                                #этот пидор не закрыл ордер не смотря на сообщение о его неуспешной отмене
                                #break # идем на повторное
                            else:
                                print('CANCEL отмена НЕ выполнена, ордер уже закрыт: ', cancel_result)
                                order_filled = True
                                break
                        elif cancel_result is not None and cancel_result['success']:  # Ордер отменен. Покупаем заново
                            print('CANCEL отмена ВЫПОЛНЕНА: ', cancel_result)
                            print('Double check: ')
                            bittrex_connector.cancel(uuid) #пытаемся закрыть повторно
                            status_ispolneniya = bittrex_connector.get_order(uuid)
                            sum_remained_to_process = status_ispolneniya['result']['QuantityRemaining']
                            sum_to_buy = sum_remained_to_process
                            if status_ispolneniya['result']['IsOpen']:  # ордер не выполнен
                                price_coeff += multiplyer_val
                                print('Новая цена покупки: ', current_price)
                            else:  # ордер исполнился с момента выполнения отмены. Так бывает. И исполнен целиком
                                print('ОРДЕР ВЫПОЛНЕН НЕ СМОТРЯ НА ОТМЕНУ: ', status_ispolneniya)
                                order_filled = True
                                break #прерывание внутреннего цикла по процессингу ордера
                elif order_v_processe_pokupki is not None and not order_v_processe_pokupki['success']:
                    print('Ошибка при выполнении покупки. Ответ False: ', order_v_processe_pokupki)
                    result['order_details']['error'] = order_v_processe_pokupki
                    result['order_details']['success'] = False
                    return result
                else:
                    if buy_attempts == 3:
                        print('Невозможно выполнить покупку за 3 попытки')
                        result['order_details']['error'] = '3 attempts made'
                        result['order_details']['success'] = False
                        return result
                    buy_attempts += 1
                    time.sleep(0.5)
            if market_name.split('-')[0].upper() == 'USDT':
                usdt_btc_market_data = bittrex_connector.get_ticker('USDT-BTC')
                btc_amount = status_ispolneniya['result']['Price'] / usdt_btc_market_data['result']['Ask']
            elif market_name.split('-')[0].upper() == 'BTC':
                btc_amount = status_ispolneniya['result']['Price']
            result['user_id'] = user_id
            result['uuid'] = uuid
            result['btc_amount'] = btc_amount
            result['order_details']['success'] = False
            result['order_details'] = status_ispolneniya
            result['open_time'] = datetime.fromtimestamp(time.time()).strftime("%Y-%m-%d %H:%M:%S.%f")

        except Exception as e:
            print('Невозможно выполнить покупку', e)
            result['order_details']['error'] = e
            result['order_details']['success'] = False
    return result

def web_version_buy(bittrex_connector, currency, amount_to_buy, user_id, timeframe = 1, porog = 2,  \
        ma_small_value = None, ma_medium_value = None, ma_porog_srabativaniya = 0.9999, increment = 0.002):
    print('Buying currency {}. Amount: {}. Increment: {}'.format(currency, amount_to_buy, increment))
    print('Timeframe: ', timeframe, 'Porog: ', porog)
    start = time.time()
    buy_order = smart_buy(bittrex_connector, currency, amount_to_buy, user_id, increment)
    end = time.time()
    if buy_order['order_details']['success']:
        db_conn = make_db_conn(PG_PARAMS)
        print('Успешно купили')
        print('Количество: ', buy_order['order_details']['result']['Quantity'])
        print('Цена покупки : ', buy_order['order_details']['result']['PricePerUnit'])
        print('Completion time: ', end - start)
        db_conn.execute_request(prepare_sql_completed_buy_order_query(buy_order, timeframe, porog, \
                                                                      ma_small_value, ma_medium_value,
                                                                      ma_porog_srabativaniya))
        db_conn.commit()
        print('Information about monitoring succesfully added to DB')
        db_conn.close()
        return buy_order
    return buy_order

def web_version_sell_all(bittrex_connector, user_id):
    sell_all(bittrex_connector, user_id)
    #sell_all_close_order(int(order_id))
    # дописать возврат результатов продажи

def web_version_sell_one(bittrex_connector, user_id, order_id):
    order_data = DB_CONNECTION.execute_request("SELECT market_name, amount FROM monitor where id = '{}';".format(int(order_id))).fetchone()
    market = order_data[0]
    amount = order_data[1]
    print(market, amount)
    result = smart_sell(bittrex_connector, user_id, \
                        market, amount)
    sell_all_close_order(int(order_id))
    return result


if __name__ == '__main__':
    bitrix_url = 'https://bittrex.com/api/v1.1/public/getmarketsummaries'
    bitrix_api_key = '3c07d7b1dba940ad95690351d081afe4'
    bitrex_api_secret = '70eac5a7bb084c2a9cb71871276aaeff'
    nonce = str(int(time.time() * 1000))
    #my_bittrex_11 = bittrex.Bittrex(bitrix_api_key, bitrex_api_secret, api_version=API_V1_1)

    #console_version(my_bittrex_11)
    #module_version()
    #sell_all(1)

