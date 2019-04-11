#coding:utf-8
import pymysql
import configparser
import json

class cal_memory:

    def __init__(self):

        conf = configparser.ConfigParser()
        conf.read("setting.conf")

        self.host = conf.get("section", "host")
        self.user = conf.get("section", "user")
        self.passwd = conf.get("section", "passwd")
        self.port = int(conf.get("section", "port"))
        self.charset = conf.get("section", "charset")
        self.database = conf.get("section", "database")
        self.select_database = conf.get("section", "select_database")

        # 读取表项组
        self.table_list = []
        selection_table = conf.get("section", "select_tables")
        with open(selection_table, "r", encoding="UTF-8") as json_file:
            selection_dict = json.load(json_file)
            for select_key in selection_dict["keys"]:
                self.table_list.append(selection_dict[select_key].copy())


    def connect_db(self):
        # ---------------------------
        print("Begin connect!")
        self.conn = pymysql.connect(host=self.host, user=self.user, passwd=self.passwd, db=self.database, charset=self.charset, port=self.port)
        self.cursor = self.conn.cursor()
        print("connect to " + self.host)
        # ---------------------------

    def disconnect_db(self):
        # ---------------------------
        print("Commit and disconnect!")
        #self.conn.commit()
        self.cursor.close()
        self.conn.close()
        print("disconnect with " + self.host)
        # ---------------------------

    def excute_sql(self, sql):
        self.cursor.execute(sql)
        results = self.cursor.fetchall()
        return results
        #for row in results:
        #    print(row)

    def generate_sql(self, table_name):
        sql = "select TABLE_NAME,TABLE_ROWS,AVG_ROW_LENGTH,DATA_LENGTH,INDEX_LENGTH from TABLES where TABLE_SCHEMA='" + self.select_database + "' and TABLE_NAME='" + table_name + "'"
        return sql

    def show_data(self):
        for table_set in self.table_list:
            sum_table = 0
            sum_index = 0
            sum_rows = 0
            for table_name in table_set:
                results = self.excute_sql(self.generate_sql(table_name))
                sum_table = int(results[0][3]) + sum_table
                sum_index = int(results[0][4]) + sum_index
                sum_rows = int(results[0][1]) + sum_rows

            print(str(table_set[0]) + "    " + str(sum_table/1000000000) + "    " + str(sum_index/1000000000) + "    " + str(sum_table/sum_rows) + "    " + str(sum_rows))





if __name__ == "__main__":
    test = cal_memory()
    test.connect_db()
    test.show_data()
    test.disconnect_db()