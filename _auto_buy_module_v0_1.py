import time
from datetime import datetime


def get_updated_rates_from_db(db_conn, market_name, last_select_time = None, limit = None):
    if last_select_time is None:
        db_updated_data_request = "SELECT last from crypto where marketname = '{}' ORDER BY id DESC LIMIT {};".format \
        (market_name, limit)
        print('Initial request', db_updated_data_request)
    else:
        db_updated_data_request = "SELECT marketname, last, query_tstamp from crypto where marketname = '{}' AND query_tstamp > '{}' ORDER BY id ASC;".format \
        (market_name, last_select_time)
    return db_conn.execute_request(db_updated_data_request) #нужен ли commit???


class Make_buy():
    def __init__(self, db_conn, market, number_of_periods = 1, num_of_periods_ma_small = 15, \
                 num_of_periods_ma_medium = 45, num_of_periods_ma_large = 50, refresh_interval = 4):
        self.refresh_interval = refresh_interval
        self.db_conn = db_conn
        self.status_comment = ''
        self.number_of_periods = number_of_periods
        self.recalculated_number_of_periofs = self.number_of_periods * self.refresh_interval
        self.market_name = market
        self.update_time = None
        self.initialized =0
        self.status = 'open'
        self.ma_small_size = int(self.recalculated_number_of_periofs * num_of_periods_ma_small)
        self.ma_medium_size = int(self.recalculated_number_of_periofs * num_of_periods_ma_medium)
        self.ma_large_size = int(self.recalculated_number_of_periofs * num_of_periods_ma_large)
        self.ma_large = self.ma_constructor(self.ma_large_size)
        # выборка максимального числа элементов из БД для инициализации
        self.limit = self.recalculated_number_of_periofs * num_of_periods_ma_large
        self.ma_large_left_elem = None # для расчета тренда
        self.ma_large_right_elem = None
        self.ma_list = []
        self.signal_sent = False
        self.initialize_and_fill_ma()
        print('MONITOR: ', self.market_name)

    def ma_constructor(self, ma_size):
        ma_list = []
        def ma_calculator(price_value):
            if len(ma_list) > ma_size - 1:
                del(ma_list[0])
            ma_list.append(price_value)
            return [round((sum(ma_list) / ma_size), 9), ma_list]
        return ma_calculator

    def calculate_ma(self, price_value):
        self.ma_large_data = self.ma_large(price_value)
        self.ma_large_value = sum(self.ma_large_data[1]) / self.ma_large_size
        self.ma_small_data = self.ma_large_data[1][:self.ma_small_size]
        self.ma_small_value = sum(self.ma_small_data) / self.ma_small_size
        self.ma_medium_data = self.ma_large_data[1][:self.ma_medium_size]
        self.ma_medium_value = sum(self.ma_medium_data) / self.ma_medium_size
        self.update_time = datetime.fromtimestamp(time.time())
        if self.initialized == 1:
            print(self.update_time)
            print('MA SMALL DATA:', self.ma_small_data[0], len(self.ma_small_data))
            print('MA MEDIUM DATA:', self.ma_medium_data[0], len(self.ma_medium_data))
            print('MA LARGE DATA:', self.ma_large_data)

    def check_is_open(self): # если по данной валюте уже есть позиция в мониторе - игнорим
        print('ПРОВЕРЯЕМ ОТКРЫТ ЛИ ОРДЕР ДЛЯ МОНИТОРИНГА')
        is_open_request = "SELECT market_name FROM monitor WHERE status = 'open';"
        open_markets = self.db_conn.execute_request(is_open_request).fetchall()
        print('open_markets ', open_markets)
        for market in open_markets:
            print('MARKETS: ', market[0])
            if market == self.market_name:
                print('ОТКРЫТ!!')
                return True
            else:
                print('НЕТ В ОТКРЫТЫХ')
                return False

    def get_prices_from_db(self):
        if self.update_time is None:
            db_updated_data_request = "SELECT last from crypto where marketname = '{}' ORDER BY id DESC LIMIT {};".format \
                (self.market_name, self.limit)
            self.update_time = datetime.fromtimestamp(time.time())
            print('SQL init request', db_updated_data_request)
        else:
            db_updated_data_request = "SELECT marketname, last, query_tstamp from crypto where marketname = '{}' AND query_tstamp > '{}' ORDER BY id ASC;".format \
                (self.market_name, self.update_time)
            print('SQL update request', db_updated_data_request)
        return self.db_conn.execute_request(db_updated_data_request)  # нужен ли commit???

    def initialize_and_fill_ma(self):
        print('ИНИЦИАЛИЗИРУЕМ MA')
        initial_data = self.get_prices_from_db()
        for record in initial_data:
            self.calculate_ma(record[0])
        if self.ma_small_value > self.ma_medium_value: # получили изначальное состояние при покупке или начале мониторинга
            # зеленая выше красной - сигнал на продажу
            self.initial_state = 'Up'
        else:
            # зеленая НИЖЕ красной - сигнал на покупку
            self.initial_state = 'Down'
        self.initialized = 1

    def check_buy_signal(self):
        #добавить проверку подтверждения - если пересекла на пограничное значение - не продавать
        if not self.check_is_open(): #если в открытых, то нет смысла дальше проверять
            if (self.ma_small_value / self.ma_medium_value > 1.005) and \
               (self.ma_medium_data[-1] / self.ma_medium_data[0]) > 1.05:
                print("SIGNAL VALUE ", self.ma_small_value / self.ma_medium_value,  self.ma_small_value / self.ma_medium_value > 1.001)
                print('IS OPEN: ', self.check_is_open())
                print('MA 15: ', self.ma_small_value, len(self.ma_small_data))
                print('MA 45: ', self.ma_medium_value, len(self.ma_medium_data))
                if self.signal_sent:
                    print('СИНАЛ НА ПОКУПКУ УЖЕ ОТПРАВЛЕН. Повторный будет после пересечения МА 15 МА 45 сверху вниз.')
                else:
                    buy_info = '{}: {} - {} {} {} {} {} {} {} {}'.format(self.update_time.strftime("%Y-%m-%d %H:%M:%S"), self.market_name, \
                                                           'PRICE: ', self.moment_price, 'MA 15: ', \
                                                           round(self.ma_small_value, 4), 'MA 45: ', round(self.ma_medium_value, 4),\
                                                           'MA15 > MA45: ', round(self.ma_small_value / self.ma_medium_value, 4))
                    print(buy_info)
                    with open('log_pokupay.txt', 'a') as the_file:
                        the_file.write(buy_info)
                        the_file.write('\n')
                    self.signal_sent = True
                    return True
            elif self.ma_small_value / self.ma_medium_value < 0.995: #настроить чувствительность
                self.signal_sent = False
                return False
        else:
            return False

    def update(self, updated_rates):
        # всю выборку целиком через fetchall()!!!!!!!!!!!!!!!
        for record in updated_rates:  # сюда отдаем
            if self.update_time < record[2]:  # получены свежие данные
                self.calculate_ma(float(record[1]))
                self.moment_price = float(record[1])
                self.check_buy_signal()
            self.update_time = record[2]  # всегда ставим последнее время обработки данных
            #self.trend = self.ma_large_data[1][0] - self.ma_large_data[1][-1]
