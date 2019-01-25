from aiohttp import web
from modules_robot import trader_stakan
from datetime import datetime
import time
from modules_robot import _process_data_to_db
#import aroon_v1_classes
PG_PARAMS = {'dbname': 'postgres',
                     'host': 'localhost',
                     'password': '',
                     'user': 'postgres'}


DB_CONNECTOR = _process_data_to_db.Db_writer(PG_PARAMS)
markets = ['USDT-BTC', 'USDT-BCC', 'USDT-BTG', 'USDT-DASH', 'USDT-ETC', 'USDT-ETH', 'USDT-LTC', 'USDT-NEO', 'USDT-OMG',\
           'USDT-XMR', 'USDT-XRP', 'USDT-ZEC']
#USER_ID = 1

# def prepare_aroon_data(market_name, interval):
#     # request_time - если запускается первый раз, то нужно наполнить данными
#     # все МА. Для этого нужно выбрать согласно лимиту назад от момента request_time,
#     # являющегося временем создания либо последнего обновления ордера
#     limit = interval#сбор данных 2 раза в минуту
#     db_updated_data_request = "SELECT last from crypto where marketname = '{}' ORDER BY id DESC LIMIT {};".format \
#             (market_name, limit)
#     #print(db_updated_data_request)
#     data = DB_CONNECTOR.execute_request(db_updated_data_request).fetchall() #нужен ли commit???
#     return data

async def pokupay(request):
    return web.FileResponse('./log_pokupay.txt')

async def choose_log(request):
    fmt_fname_date = '%Y-%m-%d'
    today_log_name = './{}_robot_log.txt'.format(datetime.fromtimestamp(time.time()).strftime(fmt_fname_date))
    yesterday_log_name = './{}_robot_log.txt'.format(datetime.fromtimestamp(time.time()).strftime(fmt_fname_date))
    today_log_url = '<p><a href = /log?log_name={}>Лог за сегодня</a><p>'.format(today_log_name)
    yesterday_log_url = '<p><a href = /log?log_name={}>Лог за вчера</a><p>'.format(yesterday_log_name)
    html_data = '{}{}'.format(today_log_url, yesterday_log_url)
    return web.Response(text=html_data, content_type = 'text/html')

async def log(request):
    payload = request.query_string.split('&')
    fname = payload[0].split('=')[1]
    # with open(fname) as f:
    #     head = [next(f) for x in range(1000)]
    #     return web.Response(text=head, content_type='text/html')
    return web.FileResponse(fname)

# async def aroon(request):
#     #payload = request.query_string.split('&')
#     #
#     # currency = payload[0].split('=')[1]
#     # amount = float(payload[1].split('=')[1])
#     # user_id = payload[2].split('=')[1]
#     # timeframe = int(payload[3].split('=')[1])
#     # porog = int(payload[4].split('=')[1])
#
#     number_of_periods = 7  # количество интервалов на котором считаем аруна
#     # ДЛЯ начала работы нужно выбрать из БД число элементов согласно максимальному значению интервала,
#     # помноженному на скорость обновления БД в минуту( = 2). Для интервала 30 в расчете выборки будет:
#     # (количество интервалов на которых считаем) * макс.значение.интервала * 2 = limit
#     period_interval = 30  # макс интервал в минутах. Минута как единица измерения периода
#     limit = number_of_periods * period_interval * 4  # выборка максимального числа элементов из БД
#
#     # надо инициализировать каждый класс, а потом ему просто дописывать обновленные данные, чтобы иметь возможность
#     # сохранять состояния и видеть их изменения.
#     # нужна функция изначального наполнения, которая видимо имеется сейчас
#     # и нужна функция дозаписи и пересчета значений.
#     aroon_text = ''
#     aroon_markets_values = {}
#     for market in markets:
#         aroon = aroon_v1_classes.Aroon(number_of_periods)
#         aroon_data = prepare_aroon_data(market, limit)
#         aroon_markets_values[market] = aroon.calculate(aroon_data)
#     for market in aroon_markets_values:
#         for aroon_interval in aroon_markets_values[market]:
#             # print(market, aroon_interval, 'down: ', aroon_markets_values[market][aroon_interval]['aroon_down'], \
#             #       'up: ', aroon_markets_values[market][aroon_interval]['aroon_up'])
#             aroon_text += "<tr><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td></tr>".format( \
#                 market, aroon_interval, aroon_markets_values[market][aroon_interval]['signal'], \
#                 aroon_markets_values[market][aroon_interval]['aroon_down'], \
#                 aroon_markets_values[market][aroon_interval]['aroon_up'])
#
#     text = '''
#     <!DOCTYPE html>
#     <html lang="en">
#         <head>
#             <meta charset="UTF-8">
#             <title>Робот. Монитор</title>
#         </head>
#         <body>
#         <h3>Арун</h3>
#         <table style="width:100%">
#               <tr>
#                 <th>Валюта</th>
#                 <th>Интервал</th>
#                 <th>Сигнал</th>
#                 <th>Down</th>
#                 <th>Up</th>
#               </tr>
#               {}
#         </table>'''.format(aroon_text)
#
#     return web.Response(text=text, content_type='text/html')

async def auth(request):
    text = '''<html>
<head>

<title>Login</title>
</head>

<body>

<form action="/choose_buy" align="center">
<br>
ID пользователя:<input type="text" name="username"><br><br><br>
<input type="Submit"  value="Submit">

</form>
</body>
</html>'''
    return web.Response(text=text, content_type = 'text/html')
    #return web.FileResponse('./index.html')

async def choose_buy(request):
    users = {
        1: 'Шурик', #alex
        2: 'Сергей' #eserj
    }
    payload = request.query_string.split('&')
    user_id = int(payload[0].split('=')[1])
    if user_id in users:
        text = '''
        <!DOCTYPE html>
        <html lang="en">
            <head>
                <meta charset="UTF-8">
                <title>Title</title>
            </head>
            <body>
                <h2>Привет, {}</h2>
                <h3>Что купить?</h3>
                <form action="/make_buy">
                  Валюта:<br>
                  <input type="text" name="currency" value="USDT-ZEC">
                  <br>
                  Количество:<br>
                  <input type="text" name="amount" value="4">
                    <br>
                  <input type="hidden" name="user_id" value="{}">
                  Интервал, минут:<br>
                  <input type="text" name="timeframe" value="1">
                    <br>    
                  <strong>Параметры алгоритма </br>выхода по средней:</br></strong>
                  Stop-loss, %:<br>
                  <input type="text" name="porog" value="2">
                    <br>
                  <strong>Параметры алгоритма </br>выхода по МА:</br></strong>
                  Значение малой МА, </br>целое число:<br>
                  <input type="text" name="ma_small_value" value="3">
                    <br>
                  Значение средней МА, </br>целое число:<br>
                  <input type="text" name="ma_medium_value" value="11">
                    <br>  
                   Порог срабатывания МА, %: </br>(точка - разделитель дробной части)<br>
                  <input type="text" name="ma_porog_srabativaniya" value="0.5">
                    <br>
                    <br>
                  <input type="submit" value="Купить!">
                </form>
                <p><a href="/show_monitor?user_id={}">Смотреть покупки</a></p>
            </body>
        </html>
            '''.format(users[user_id], user_id, user_id)
    else:
        text = '''
        <!DOCTYPE html>
            <html lang="en">
            <head>
                <meta charset="UTF-8">
                <title>Title</title>
            </head>
            <body>
                <h2>Похоже, пользователя с ID {} еще не существует</h2>
                <a href = '/auth'>Попробовать еще раз</a>
            </body>
            </html>
    '''.format(user_id)
    return web.Response(text=text, content_type = 'text/html')


def make_buy(request):
    payload= {}
    for item in request.query_string.split('&'):
        payload[item.split('=')[0]] = item.split('=')[1]
    print('PAYLOAD: ', payload)

    currency = payload['currency']
    amount = float(payload['amount'])
    user_id = payload['user_id']
    timeframe = int(payload['timeframe'])
    porog = float(payload['porog'])
    ma_small_value = int(payload['ma_small_value'])
    ma_medium_value = int(payload['ma_medium_value'])
    ma_porog_srabativaniya = float(payload['ma_porog_srabativaniya'])


    bitrex_connector = trader_stakan.create_personalized_bittrex_connection(_process_data_to_db.Db_writer(PG_PARAMS),
                                                                            user_id)
    buy_result = trader_stakan.web_version_buy(bitrex_connector, currency, amount, user_id, timeframe, porog, \
                                        ma_small_value, ma_medium_value, ma_porog_srabativaniya)
    #print(currency, amount)
    print('BUY RESULT', buy_result)
    html_string = '<h3><a href = /show_monitor?user_id={}>Страница покупок</a></h3>\
                   <h3><a href = /choose_buy?user_id={}>Купить еще что нибудь</a></h3>\
                   <h2>Результат покупки</h2>'.format(user_id, user_id)
    for param in buy_result['order_details']['result']:
        html_string += '{}{}{}{}{}'.format('<p>', param, ': ', buy_result['order_details']['result'][param], '</p>')
    #print(html_string)
    #return web.json_response(buy_result)
    return web.Response(text=html_string, content_type='text/html')


def sell_all(request):
    payload = request.query_string.split('&')
    print('request', payload)
    user_id = payload[0].split('=')[1]
    bitrex_connector = trader_stakan.create_personalized_bittrex_connection(_process_data_to_db.Db_writer(PG_PARAMS),
                                                                            user_id)
    sell_result = trader_stakan.web_version_sell_all(bitrex_connector, user_id)
    # print(currency, amount)
    return web.json_response(sell_result)

def sell_one(request):
    payload = request.query_string.split('&')
    #print('request', payload)
    print('WEB INTERFACE SELL INITIATED===============================')
    order_id = float(payload[0].split('=')[1])
    user_id = float(payload[1].split('=')[1])
    bitrex_connector = trader_stakan.create_personalized_bittrex_connection(_process_data_to_db.Db_writer(PG_PARAMS),
                                                                            user_id)
    sell_one_result = trader_stakan.web_version_sell_one(bitrex_connector, user_id, order_id)
    return web.json_response(sell_one_result)

def show_monitor(request):
    payload = request.query_string.split('&')
    user_id = payload[0].split('=')[1]
    total_profit = 0
    fmt = '%d %b %H:%M'
    fmt_update = '%H:%M'
    db_conn = _process_data_to_db.Db_writer(PG_PARAMS)
    open_orders_sql_request_ss = "SELECT market_name, amount, open_time, open_price, sl, \
    update_time, profit, timeframe, porog, status, id, btc_amount, algo, \
    ma_small_value, ma_medium_value, ma_porog_srabativaniya, current_price \
    FROM monitor WHERE status = 'open' AND user_id = '{}' ORDER BY id DESC;".format(user_id)
    open_orders = db_conn.execute_request(open_orders_sql_request_ss)
    db_conn.commit()
    active_orders = ''
    history = ''
    if open_orders.rowcount > 0:
        print('Open rowcount ', open_orders.rowcount)
        sql_response_open = open_orders.fetchall()
        active_orders = ''
        for record in sql_response_open:
            currency = record[0]
            amount = record[1]
            open_time = record[2]
            open_price = record[3]
            stop_loss = record[4]
            update_time = record[5]
            profit = record[6]
            timeframe = record[7]
            porog = record[8]
            status = record[9]
            order_id = int(record[10])
            btc_amount = round(float(record[11]), 6)
            algo = record[12]
            ma_small_value = record[13]
            ma_medium_value = record[14]
            ma_porog_srabativaniya = record[15]
            current_price = record[16]
            # print('Open record: ', record)
            market = '<a href = sell_one?order_id={}&user_id={}>{}</a>'.format(order_id, user_id, currency)
            active_orders += "<tr><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td></tr>".format( \
                market, round(amount, 6), btc_amount, open_time, open_price, round(stop_loss, 10), round(current_price, 10), update_time, round(profit, 3),
                algo, ma_small_value, ma_medium_value, ma_porog_srabativaniya, timeframe, porog)
            total_profit += round(record[6], 3)

    #ОРДЕРА С ОШИБКОЙ
    db_conn = _process_data_to_db.Db_writer(PG_PARAMS)
    error_orders_sql_request_ss = "SELECT market_name, amount, open_time, open_price, update_time, sl, profit, timeframe, \
                                  status, id, comment FROM monitor WHERE\
                                  status !='open' AND status !='closed' AND user_id = '{}' ORDER BY id DESC;".format(user_id)
    error_orders = db_conn.execute_request(error_orders_sql_request_ss)
    db_conn.commit()
    error_table = ''
    error_html = ''
    if error_orders.rowcount > 0:
        print('Error rowcount ', error_orders.rowcount)
        sql_response_error = error_orders.fetchall()
        for record in sql_response_error:
            currency = record[0]
            amount = record[1]
            open_time = record[2]
            open_price = record[3]
            update_time = record[4]
            #sl = record[5]
            profit = record[6]
            timeframe = record[7]
            status = record[8]
            order_id = int(record[9])
            comment = record[10]
            # market = '<a href = sell_one?order_id={}&user_id={}>{}</a>'.format(order_id, user_id, currency)
            error_table += "<tr><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td></tr>".format( \
                order_id, currency, amount, open_time.strftime(fmt), open_price, update_time.strftime(fmt_update), round(profit, 3),
                timeframe, status, comment)
            error_html = '''
                    <h2>Ошибки:</h2>
                    <table style="width:100%" border="1">
                      <tr>
                        <th>ID</th>
                        <th>Валюта</th>
                        <th>Количество</th>
                        <th>Время покупки</th> 
                        <th>Цена покупки</th>
                        <th>Обновление</th>
                        <th>Профит, %</th>
                        <th>Интервал, </br>мин.</th>
                        <th>Статус</th>
                        <th>Инфо по </br>статусу</th>
                      </tr>
                      {}
                    </table>'''.format(error_table)
    closed_orders_sql_request = "SELECT market_name, amount, open_time, open_price, sl, \
    update_time, profit, timeframe, porog, algo, id, btc_amount FROM monitor WHERE status = 'closed' \
    AND user_id = '{}' ORDER BY id DESC;".format(user_id)
    closed_orders = db_conn.execute_request(closed_orders_sql_request)
    db_conn.commit()

    if closed_orders.rowcount > 0:
        sql_response_closed = closed_orders.fetchall()
        history = ''
        for record in sql_response_closed:
            currency = record[0]
            amount = record[1]
            open_time = record[2]
            open_price = record[3]
            stop_loss = record[4]
            update_time = record[5]
            profit = record[6]
            timeframe = record[7]
            porog = record[8]
            algo = record[9]
            order_id = int(record[10])
            btc_amount = round(float(record[11]), 6)
            # print('Closed record: ', record)
            history += "<tr><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td></tr>".format( \
                currency, amount, btc_amount, open_time, open_price, round(stop_loss, 9), update_time,
                round(profit, 3), timeframe, porog, algo)

    text = '''<!DOCTYPE html>
        <html lang="en">
        <head>
            <meta charset="UTF-8">
            <title>Робот. Монитор</title>
        </head>
        <body>
        <h3>Активные позиции</h3>
        <table style="width:100%"  border="1">
              <tr>
                <th>Валюта</th>
                <th>Коли-</br>чество</th>
                <th>Количес-</br>тво BTC</th>
                <th>Время </br>покупки</th> 
                <th>Цена </br>покупки</th>
                <th>Стоп </br>лосс</th>
                <th>Тек. ср.</br>цена</th>
                <th>Обнов-</br>ление</th>
                <th>Профит, </br>%</th>
                <th>Алго</th>
                <th>МА </br>мал</th>
                <th>Ма </br>сред</th>
                <th>Ма </br>порог%</th>
                <th>Интер-</br>вал, мин</th>
                <th>Прода-</br>жа </br>% </th>
              </tr>
              {}
            </table>
        <h3>Профит по открытым позициям: {}%</h3>
        <h3><a href = sell_all?user_id={}>Продать все!</a></h3>
        <h3><a href = /choose_buy?user_id={}>Купить еще что-нибудь</a></h3>
                {}
        <h3><a href = /choose_log>Смотреть логи робота</a></h3>
        <h2>История</h2>
        <table style="width:100%" border="1">
              <tr>
                <th>Валюта</th>
                <th>Количество</th>
                <th>Количество BTC</th>
                <th>Время покупки</th> 
                <th>Цена покупки</th>
                <th>Стоп лосс</th>
                <th>Обновление</th>
                <th>Профит, %</th>
                <th>Интервал, </br>мин.</th>
                <th>Продажа % </th>
                <th>Алго</th>
              </tr>
              {}
            </table>    
        </body>
        </html>'''.format(active_orders, total_profit, user_id, user_id, error_html, history)
    db_conn.close()
    return web.Response(text=text, content_type='text/html')