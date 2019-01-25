import time
from datetime import datetime
# import trader

END_DATA = datetime.fromtimestamp(time.time())

def get_updated_rates_from_db(db_conn, market_name, update_time = None, limit = 1200):
    db_updated_data_request = "SELECT last, query_tstamp from crypto where marketname =\
    '{}' AND query_tstamp > '{}' AND query_tstamp < '{}' ORDER BY id ASC limit {};".format(market_name, update_time, END_DATA, limit)
    return db_conn.execute_request(db_updated_data_request) #нужен ли commit???


class Make_buy():
    def __init__(self, db_conn, market_name, start_time):
        self.db_conn = db_conn
        self.buy_time = start_time
        self.long_ma_list = []
        self.speed_stop_length = 3
        self.speed_stop_list = []
        self.first_buy_made = False
        self.speed_stop_iter_counter = 0
        self.speed_stop_minute_value = 0
        self.speed_stop_sell_signal = 0
        self.aroon_dict = {}
        self.market_name = market_name
        self.update_time = start_time
        self.init_list = 0
        self.total_profit_value = 0
        self.moment_price = 1 # текущая цена, последнее неусредненное значение полученных данных.
        # От нее считаем только профит
        self.speed_sell_list = []
        self.speed_sell_iter_counter = 0
        self.speed_sell_minute_value = 0
        self.speed_sell_length = 3
        self.speed_sell_activated = 0
        self.speed_sell_activation_threshold = 7 #в процентах
        self.speed_sell_delta_to_sell = 1.5 #в процентах
        self.speed_sell_signal = 0 #  сигнал к продаже
        self.speed_sell_signal_sent = 0
        self.speed_stop_list = []
        self.speed_buy_list = []
        self.speed_buy_iter_counter = 0
        self.speed_buy_minute_value = 0
        self.speed_buy_length = 3
        self.speed_buy_activated = 0
        self.speed_buy_activation_threshold = 7 #в процентах
        self.speed_buy_delta_to_sell = 1.5 #в процентах
        self.speed_buy_signal = 0 #  сигнал к продаже
        self.speed_buy_signal_sent = 0
        self.num_of_periods_ma_small = 3
        self.num_of_periods_ma_medium = 50
        self.signal_sell = 0
        self.signal_buy = 0
        self.refresh_interval = 4
        self.number_of_periods = 3
        self.limit = self.num_of_periods_ma_medium * self.refresh_interval
        self.recalculated_number_of_periofs = self.number_of_periods * self.refresh_interval
        self.ma_small_size = int(self.recalculated_number_of_periofs * self.num_of_periods_ma_small)
        self.ma_medium_size = int(self.recalculated_number_of_periofs * self.num_of_periods_ma_medium)
        self.limit = self.ma_medium_size
        self.ma_medium = self.ma_constructor(self.ma_medium_size) #!!!!! создается функция расчета средней
        self.initialized = 0
        #aroon:
        self.temp_period_val_list = []
        self.period_counter = 0
        self.dlina_aroona = 14 #размер длины аруна
        self.interval_list = [7]
        self.prepare_aroon_dict()
        #self.refresh_interval = refresh_interval
        self.aroon_objects_dict = {}
        self.aroon_signal = 0
        self.initialize_and_fill_ma()
        self.current_price = self.ma_medium_data[1][-1]
        # self.current_price = 0
        self.speed_stop_activation_threshold = 0.1  # в процентах
        self.speed_stop_activated = 1
        self.total_profit = []
        self.buy_price = 0
        self.stop_loss_porog = 0.03
        self.stop_loss = self.current_price * self.stop_loss_porog
        self.last_price = 0
        self.signal = ''

    def initialize_and_fill_ma(self):
        print('initializeng MA')
        initial_data = self.get_prices_from_db().fetchall()
        print("initial_data: ", initial_data)
        for record in reversed(initial_data):
            #print(record[0])
            self.calculate_ma(record)
            self.calculate_aroon(record[0])
        self.print_ma()
        self.initialized = 1

    def prepare_aroon_dict(self):
        for interval in self.interval_list:
            aroon_dict_key = 'aroon_{}'.format(self.number_of_periods)
            self.aroon_dict[aroon_dict_key] = {
                'aroon_list': [],
                'aroon_down': 0,
                'aroon_up': 0,
                'signal': ''
            }

    def calculate_aroon(self, price):
        print('Calculating aroon: ', price)
        number_of_periods = 9
        #self.temp_period_val_list = []
        aroon_dict_key = 'aroon_{}'.format(self.number_of_periods)
#        for price_index, price_value in enumerate(price_list):
        self.temp_period_val_list.append(price)
        self.period_counter += 1
        if self.period_counter > number_of_periods:
            self.period_counter = 0
            if len(self.temp_period_val_list) > self.number_of_periods * self.refresh_interval: # посчитали, что у нас данных в минуту в 4 раза больше
                self.aroon_dict[aroon_dict_key]['aroon_list'].append(sum(self.temp_period_val_list) / (self.number_of_periods * self.refresh_interval))
                self.temp_period_val_list = []
            if len(self.aroon_dict[aroon_dict_key]['aroon_list']) > self.dlina_aroona: # укорачиваем длину списка
                self.aroon_dict[aroon_dict_key]['aroon_list'] = self.aroon_dict[aroon_dict_key]['aroon_list'][-self.dlina_aroona:]
                index_min = min(range(len(self.aroon_dict[aroon_dict_key]['aroon_list'])), \
                                key=self.aroon_dict[aroon_dict_key]['aroon_list'].__getitem__)
                index_max = max(range(len(self.aroon_dict[aroon_dict_key]['aroon_list'])), \
                                key=self.aroon_dict[aroon_dict_key]['aroon_list'].__getitem__)
                self.aroon_dict[aroon_dict_key]['aroon_down_index'] = index_min + 1 # считается от 0
                self.aroon_dict[aroon_dict_key]['aroon_up_index'] = index_max + 1# считается от 0
                self.aroon_dict[aroon_dict_key]['aroon_down'] = \
                    (self.number_of_periods - (number_of_periods - (index_min + 1))) / number_of_periods * 100
                self.aroon_dict[aroon_dict_key]['aroon_up'] = \
                    (self.number_of_periods - (number_of_periods - (index_max + 1))) / number_of_periods * 100
                if self.aroon_dict[aroon_dict_key]['aroon_up'] - self.aroon_dict[aroon_dict_key]['aroon_down'] == 0:
                    self.aroon_dict[aroon_dict_key]['signal'] = 'neutral'
                elif self.aroon_dict[aroon_dict_key]['aroon_up'] - self.aroon_dict[aroon_dict_key]['aroon_down'] < 50:
                    self.aroon_dict[aroon_dict_key]['signal'] = 'average sell'
                elif self.aroon_dict[aroon_dict_key]['aroon_up'] - self.aroon_dict[aroon_dict_key]['aroon_down'] < 75:
                    self.aroon_dict[aroon_dict_key]['signal'] = 'strong sell'
                elif self.aroon_dict[aroon_dict_key]['aroon_up'] - self.aroon_dict[aroon_dict_key]['aroon_down'] > 50:
                    self.aroon_dict[aroon_dict_key]['signal'] = 'average buy'
                elif self.aroon_dict[aroon_dict_key]['aroon_up'] - self.aroon_dict[aroon_dict_key]['aroon_down'] > 75:
                    self.aroon_dict[aroon_dict_key]['signal'] = 'strong buy'
                self.aroon_signal = self.aroon_dict[aroon_dict_key]['aroon_up'] - self.aroon_dict[aroon_dict_key][
                    'aroon_down']
            # print(aroon_dict_key, 'длина списка значений: ', len(self.aroon_dict[aroon_dict_key]['aroon_list']))
            # print(aroon_dict_key, 'index_max: ', index_max, number_of_periods)
            # print(aroon_dict_key, 'index_min: ', index_min, number_of_periods)
            #interpreting signals:
    #return self.aroon_dict

    def init_lists(self, price):
        self.speed_sell_list.append(price)
        self.speed_buy_list.append(price)
        self.speed_stop_list.append(price)

    def get_prices_from_db(self):
        if self.initialized == 0:
            db_updated_data_request = "SELECT last, query_tstamp from crypto where marketname =\
            '{}' AND query_tstamp < '{}' ORDER BY id DESC LIMIT {};".format(self.market_name, self.update_time, self.limit)
            print('SQL init request', db_updated_data_request)
        else:
            db_updated_data_request = "SELECT last, query_tstamp from crypto where marketname =\
            '{}' AND query_tstamp > '{}' AND query_tstamp < '{}' ORDER BY id ASC LIMIT 1200;".format(self.market_name, self.update_time, END_DATA)
            print('SQL update request', db_updated_data_request)
        return self.db_conn.execute_request(db_updated_data_request)  # нужен ли commit???

    def write_log(self, str_to_write):
        fmt_fname_date = '%Y-%m-%d'
        fmt_row_date = "%Y-%m-%d %H:%M:%S"
        fname = '{}_{}_pokupay_log.txt'.format(datetime.fromtimestamp(time.time()).strftime(fmt_fname_date), self.market_name)
        #date_to_row = '{}\n'.format(datetime.fromtimestamp(time.time()).strftime(fmt_row_date))
        log_str = '{}'.format(str_to_write)
        try:
            with open(fname, 'r') as original:
                #print('writing to LOG: {}'.format(
                #    '{}_robot_log.txt'.format(datetime.fromtimestamp(time.time()).strftime(fmt_fname_date))))
                data = original.read()
            with open(fname, 'w+') as modified:
                modified.write(log_str + data)
                modified.write('\n')
        except Exception as e:
            with open(fname, 'w+') as original:
                date_to_row = '{}\n'.format(datetime.fromtimestamp(time.time()).strftime(fmt_row_date))
                original.write(date_to_row)

    def ma_constructor(self, ma_size):
        ma_list = []
        def ma_calculator(price_value):
            if len(ma_list) > ma_size - 1:
                del(ma_list[0])
            ma_list.append(price_value)
            return [(sum(ma_list) / ma_size), ma_list]
        return ma_calculator

    def calculate_ma(self, record):
        print('Calculate MA, RECORD: ', record)
        self.ma_medium_data = self.ma_medium(record[0]) # ТЕПЕРЬ ЭТО ПО СРЕДНЕЙ
        self.ma_small_data = self.ma_medium_data[1][-self.ma_small_size:]
        self.ma_small_value = sum(self.ma_small_data) / self.ma_small_size
        self.ma_medium_value = self.ma_medium_data[0]
        self.long_ma_list.append(self.ma_medium_value)
        if len(self.long_ma_list) > 2:
            del(self.long_ma_list[0])
        self.update_time = record[1]
        #self.calculate_speed_stop(price_value)
        #if self.initialized == 1:
        #    self.print_ma()

    def print_ma(self):
        fmt_row_date = "%Y-%m-%d %H:%M:%S"
        print('{}'.format(datetime.fromtimestamp(time.time()).strftime(fmt_row_date)))
        print('{} - Order ID: {}. Interval: {}, MA_POROG: {}'.format(self.market_name, "self.order_id", self.number_of_periods, "self.ma_porog_srabativaniya"))
        print('MA_SMALL: ', "self.ma_small_value", "len(self.ma_small_data)", 'MA SMALL PERIOD: ', "self.num_of_periods_ma_small")
        #print('MA_SMALL DATA: ', self.ma_small_data)
        # print('MA_MED: ', self.ma_medium_value, len(self.ma_medium_data[1]), 'MA MED PERIOD: ', self.num_of_periods_ma_medium)
        #print('MA_MED DATA: ', self.ma_medium_data[1])
        #print('MA_MED SIZE', self.ma_medium_size)
        #print('self.open_time', self.open_time)

    def update_stop_loss(self):
        if self.ma_medium_data[1][-1] > self.current_price:
            self.current_price = self.ma_medium_data[1][-1]
            self.stop_loss = self.current_price * self.stop_loss_porog

    def calculate_speed_stop(self, price_val):
        self.speed_stop_minute_value += price_val
        self.speed_stop_iter_counter += 1
        if self.speed_stop_iter_counter % self.refresh_interval == 0:
            self.speed_stop_list.append(self.speed_stop_minute_value)
            self.speed_stop_minute_value = 0
            self.speed_stop_iter_counter = 0
            if len(self.speed_stop_list) > self.speed_stop_length:
                del(self.speed_stop_list[0])
        #данные заполнены еще при инициализации, можно рассчитывать.
        #1) этот спид-стоп должен быть активирован. Активация 1 - это рост профита
        # выше self.speed_stop_activation_threshold, для начала
        if self.speed_stop_activated == 1:
            if ((1 - (self.speed_stop_list[-2] / self.speed_stop_list[-1])) * 100 > 2) or \
               ((1 - (self.speed_stop_list[0] / self.speed_stop_list[-1])) * 100 > 2):
                    self.speed_stop_sell_signal = 1

    def calculate_speed_sell(self, price_val):
        #print('Calculating speed sell')
        # self.number_of_periods
        self.speed_sell_minute_value += price_val
        self.speed_sell_iter_counter += 1
        if self.speed_sell_iter_counter % (self.refresh_interval * self.number_of_periods) == 0: # полминуты
            self.speed_sell_list.append(self.speed_sell_minute_value/ self.refresh_interval)
            self.speed_sell_minute_value = 0
            self.speed_sell_iter_counter = 0
            if len(self.speed_sell_list) > self.speed_sell_length:
                del(self.speed_sell_list[0])
        if (self.aroon_signal < - 85): #AROON
            self.signal = 'Aroon < -50'
            if self.first_buy_made:
                if (self.ma_small_value / self.ma_medium_value) < 0.97:
                    self.signal = 'MAsmall {} < MAmed {}'.format(self.ma_small_value, self.ma_medium_value)
                elif (((self.speed_sell_list[1] / self.speed_sell_list[2])) > 1.007):
                     #(self.speed_sell_list[0] / self.speed_sell_list[1])) > 1.001):
                    self.signal = 'Speed sell'
                elif self.ma_medium_data[1][-1] < self.stop_loss:
                    self.signal = 'StopLoss'
                # elif self.speed_stop_sell_signal == 1:
                #     self.signal = 'SPEEDstop'
                if self.last_price != self.moment_price and self.speed_sell_signal == 0 and \
                    self.signal != '':
                    if self.buy_price != 0:
                        self.total_profit.append((1 - (self.buy_price / self.moment_price)) * 100)
                        print('profit: ', (1 - self.buy_price / self.moment_price) * 100 )
                        self.total_profit_value = sum(self.total_profit)
                    self.speed_sell_signal = 1
                    self.speed_buy_signal = 0
                    self.last_price = self.moment_price
                    log1 = '{}. Time: {}. {}. Price: {}, \nProfit: {}\n'.format(self.market_name, self.update_time, self.signal,
                                                                                self.moment_price, self.total_profit_value)
                    #log2 = self.speed_sell_list
                    print(log1)
                    write_to_file_string = '\n{}\n'.format(log1)
                    self.write_log(write_to_file_string)
        self.signal = ''

# NEW SPEED BUY=========
    def calculate_speed_buy(self, price_val):
        fmt_row_date = "%Y-%m-%d %H:%M:%S"
        #print('Calculating speed buy')
        self.speed_buy_minute_value += price_val
        self.speed_buy_iter_counter += 1
        if self.speed_buy_iter_counter % self.refresh_interval == 0: # полминуты
            self.speed_buy_list.append(self.speed_buy_minute_value/self.refresh_interval)
            self.speed_buy_minute_value = 0
            self.speed_buy_iter_counter = 0
            if len(self.speed_buy_list) > self.speed_buy_length:
                del(self.speed_buy_list[0])
            if self.aroon_signal > 75: #AROON
                self.signal = 'Aroon > 80'
            if self.ma_small_value > self.ma_medium_value and self.ma_small_value / self.ma_medium_value > 1.005:
                self.signal = 'MAsmall {} > MAmed {}'.format(self.ma_small_value, self.ma_medium_value)
            # if self.speed_buy_list[2] > self.speed_buy_list[1] and self.speed_buy_list[1] > self.speed_buy_list[0]:
            # if ((self.speed_buy_list[2] / self.speed_buy_list[0] > 1.015) and \
            if self.long_ma_list[1] > self.long_ma_list[0]:
                print('++++++self.buy_time, fmt_row_date) - datetime.strptime(self.update_time, fmt_row_date\n',
                      (datetime.strptime(self.buy_time, fmt_row_date) - datetime.strptime(self.update_time, fmt_row_date)).total_seconds())
                if     (self.speed_buy_list[1] / self.speed_buy_list[0] > 1.004) and \
                       (self.speed_buy_list[2] / self.speed_buy_list[1] > 1.01):
                    # (self.speed_buy_list[2] / self.speed_buy_list[1] > 1.005) and \
                    self.signal = 'Speed buy'
                if (price_val != self.last_price and \
                    self.speed_buy_signal == 0 and self.signal != '') and \
                        (datetime.strptime(self.buy_time, fmt_row_date) - datetime.strptime(self.update_time, fmt_row_date)).total_seconds() > 120:
                        self.speed_buy_signal = 1
                        self.buy_time = self.update_time
                        self.speed_sell_signal = 0
                        self.first_buy_made = True
                        self.buy_price = self.moment_price
                        self.last_price = self.moment_price
                        log1 = '{}. Time: {}. {}. Price: {}. \nProfit: {}'.format(self.market_name, self.update_time, self.signal,
                                                                    self.moment_price, self.total_profit_value)
                        #log2 = self.speed_sell_list
                        print(log1)
                        write_to_file_string = '\n{}\n'.format(log1)
                        self.write_log(write_to_file_string)
        self.signal = ''



    # AVERAGE PER INTERVAL НЕ МЕНЯЛ НАЗВАНИЕ СЧИТАЕМ МА============================
    def calculate(self, updated_rates):
        fmt_row_date = "%Y-%m-%d %H:%M:%S"
        counter = 0
        if self.init_list == 0:
            for record in updated_rates:
                self.init_lists(record[0])
                counter += 1
                if counter == 3:
                    self.init_list = 1
                    break
        for record in updated_rates:
            #print(record)
            self.calculate_aroon(record[0])
            self.calculate_ma(record) # обновляем средние
            self.moment_price = record[0]
            self.update_stop_loss()
            self.update_time = record[1].strftime(fmt_row_date)
            self.calculate_speed_stop(record[0])
            self.calculate_speed_sell(record[0])
            self.calculate_speed_buy(record[0])
            print(self.update_time)
            print('AROON signal: ', self.aroon_signal)
            if len(self.total_profit) > 0:
                print('Total profit: ', self.total_profit_value)
