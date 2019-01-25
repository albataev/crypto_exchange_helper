import psycopg2
import ujson
import time


class Format_resp():
    def __init__(self, resp_text = None, data_type = 'JSON'):
        self.resp = resp_text
        self.data_type = data_type

    def do(self, resp = None, new_items = None, json_key = None):
        if self.data_type == 'JSON':
            #new items - if we need to add some specific data to the API response before writing it to the DB
            if resp is not None:
                try:
                    if json_key is None:
                        data = ujson.loads(resp)
                    else:
                        data = ujson.loads(resp)[json_key]
                    if new_items is None:
                        # print('returning unchainged')
                        return data
                    else:
                        # print('Appending new data to json items: ', new_items)
                        for data_item in data:
                            for key in new_items:
                                data_item[key] = new_items[key]
                    return data
                except Exception as ex:
                    print('Exception during processing JSON: {}'.format(ex))
            else:
                print('got none to JSON processing {}')
                return None
        else:
            pass


class Writer:
    def execute_request(self, sql_part):
        print('not implemented')

class Db_writer(Writer):
    def __init__(self, connect_params, db_type = 'postgres'):
        print("Connect params: ", connect_params)
        self.db_type = db_type
        self.connect_params = connect_params
        if db_type == 'postgres':
            self.connect()
        else:
            pass

    def check_connection(self):
        # проверить связь с БД, на случай закрытия соединения нужно переинициализировать
        pg_write = None
        while pg_write is None:
            try:
                self.cur.execute('SELECT 1;')
                self.dbconn.commit()
                pg_write = 'ok'
            except psycopg2.OperationalError as e:
                print('Error during DB request, attempting to reconnect: ', e)
                time.sleep(5)
                self.connect()
        return True

    def connect(self):
        # проверяем доступность сервера. Если недоступен - пытаемся через паузу повторить попытку
        if self.db_type == 'postgres':
            connected = False
            while connected == False:
                try:
                    self.dbconn = psycopg2.connect(**self.connect_params)
                    self.cur = self.dbconn.cursor()
                    print('Writer Connected to the PostgreSQL database')
                    connected = True
                    return self.cur
                except Exception as e:
                    print('Error in initialisation DB connection: ', e)
                    print('Trying to reconnect')
                    time.sleep(5)

    def execute_request(self, sql_query):
        if self.db_type == 'postgres':
            try:
                self.cur.execute(sql_query)
            except Exception as e:
                print('Error executing query: ', e, 'attempting to reconnect')
                time.sleep(5)
                self.connect()
        else:
            print('other DB adapter not implemented')
        return self.cur

    def commit(self):
        self.dbconn.commit()

    def close(self):
        if self.db_type == 'postgres':
            self.cur.close()
            self.dbconn.close()

if __name__ == '__main__':
    pass

