#!/usr/bin/python
# -*- coding: UTF-8 -*-
import pymysql
from prettytable import PrettyTable

''' python 3.5.2 mysql 5.7测试通过'''


class dbclass:
    def __init__(self,host,port,user,password,db):
        self.db=db
        self.host=host
        self.conn = pymysql.connect(host=host, user=user, password=password, database=db, port=port,charset='utf8')
        self.cursor = self.conn.cursor()


    def intdb(self):
        try:
            sql_c1 = '''
                CREATE TABLE found_values (
                table_name varchar(30) not null primary key,
                recs int not null,
                crc_sha varchar(100) not null,
                crc_md5 varchar(100) not null
                ) '''
            self.cursor.execute(sql_c1)
            sql_c3 = 'CREATE TABLE tchecksum (chk char(100)) ENGINE=blackhole;'
            self.cursor.execute(sql_c3)

            sql_c5 = 'CREATE TABLE s1_values LIKE found_values;'
            sql_c7 = 'CREATE TABLE s2_values LIKE found_values;'
            sql_c9 = 'CREATE TABLE noprimary_table (table_name varchar(30) not null primary key);'


            self.cursor.execute(sql_c5)
            self.cursor.execute(sql_c7)
            self.cursor.execute(sql_c9)
            print('--------------------------------------------------------------------------------------------------------')
            print('   {} init 5 tables ok:found_values,tchecksum,s1_values,s2_values,noprimary_table'.format(self.host))
            print('--------------------------------------------------------------------------------------------------------')

        except Exception  as e:
            print('--------------------------------------------------------------------------------------------------------')
            print('   {} init 5 tables error:'.format(self.host)+str(e))
            print(   '请人工处理后再执行.....')
            print('--------------------------------------------------------------------------------------------------------')

    def get_checksum(self):
        self.cursor.execute('SHOW TABLES')
        tables=[t[0] for t in  self.cursor.fetchall()]


        for table in tables:
            cols = self.get_cols(table)
            pri_id = self.get_pri(table)
            if not pri_id:
                print('{} skip table {} !!!!!!!'.format(self.host,table))
                nopri_sql = "insert into noprimary_table (table_name) values('{}')".format(table)
                self.cursor.execute(nopri_sql)
                self.conn.commit()
                continue 

            if table  in ['noprimary_table','found_values','s1_values','s1_values']:
                continue
            self.cursor.execute("SET @crc= '';")
            cstr = "'#'"+','+'@crc'+ ','+','.join(cols)
            crc_sql="INSERT INTO tchecksum  SELECT @crc := sha(CONCAT_WS({})) FROM {} ORDER BY {};".format(cstr,table,pri_id)

            self.cursor.execute(crc_sql)
            self.conn.commit()
            self.cursor.execute("SELECT @crc;")
            crc_result = self.cursor.fetchone()
            if crc_result[0]:
                crc_result = crc_result[0]
            else:
                crc_result = 'ZZZZZZZZZZZZZZZZ'

            sql_result_ins="INSERT INTO found_values VALUES ('{}', (SELECT COUNT(*) FROM {}),'{}','{}');".format(table,table,crc_result,crc_result)
            self.cursor.execute(sql_result_ins)
            self.conn.commit()
 
            print('{} process table {}'.format(self.host,table))


              

    def get_result(self):
        self.cursor = self.conn.cursor(cursor=pymysql.cursors.DictCursor)
        sql = 'select * from found_values;'
        self.cursor.execute(sql)
        rows = self.cursor.fetchall()
        return rows



    def get_cols(self,table):
        sql="select COLUMN_NAME from information_schema.COLUMNS where table_name = '{}' and table_schema = '{}' ".format(table,self.db)
        self.cursor.execute(sql)
        result = [cols[0] for cols in self.cursor.fetchall()]
        return result

    def get_pri(self,table):
        sql="select COLUMN_NAME from information_schema.COLUMNS where table_name = '{}' and table_schema = '{}'  and COLUMN_KEY='PRI' ".format(table,self.db)
        self.cursor.execute(sql)
        result = self.cursor.fetchone()
        if result:
            return result[0]
        else:
            return None
        

    def cmp_result(self,r1,r2):
        x = PrettyTable(field_names=["s1_table_name","s1_recs","s2_table_name","s2_recs","records_match","crc_match"])
        x.align["name"] = "l"
        x.padding_width = 3



        y = PrettyTable(field_names=["computation_time"])
        y.align["name"] = "l"
        y.padding_width = 3


        for d1 in r1:
            table_name = d1['table_name']
            recs =  int(d1['recs'])
            crc_sha = d1['crc_sha']
            sql_ins1="INSERT INTO s1_values VALUES ('{}',{},'{}','{}')".format(table_name,recs,crc_sha,crc_sha)
            self.cursor.execute(sql_ins1)
            self.conn.commit()

       
        for d2 in r2:
            table_name = d2['table_name']
            recs =  int(d2['recs'])
            crc_sha = d2['crc_sha']
            sql_ins2="INSERT INTO s2_values VALUES ('{}',{},'{}','{}')".format(table_name,recs,crc_sha,crc_sha)
            self.cursor.execute(sql_ins2)
            self.conn.commit()

        summary_sql = " select s1.table_name as s1_table_name,s1.recs as s1_recs,s2.table_name as s2_table_name,s2.recs as s2_recs,IF(s1.recs=s2.recs,'OK', 'not ok') AS records_match,IF(s1.crc_sha=s2.crc_sha,'ok','not ok') AS crc_match  from s1_values s1 left join s2_values s2 on s1.table_name=s2.table_name;"
        self.cursor.execute(summary_sql)
        summary_result = self.cursor.fetchall()

        
  
        computation_time_sql = "select timediff(now(),(select create_time from information_schema.tables where table_schema='{}' and table_name='found_values')) as computation_time;".format(self.db)
        self.cursor.execute(computation_time_sql)
        time_result = self.cursor.fetchone()['computation_time']
        y.add_row([time_result])

          
        for i in range(len(summary_result)):
            s1_table_name= summary_result[i]['s1_table_name']
            s1_recs = summary_result[i]['s1_recs']
            s2_table_name = summary_result[i]['s2_table_name']
            s2_recs = summary_result[i]['s2_recs']
            records_match = summary_result[i]['records_match']
            crc_match = summary_result[i]['crc_match']
            x.add_row([s1_table_name,s1_recs,s2_table_name,s2_recs,records_match,crc_match])

        print('\n')
        print('                                                     summary')

        print(x.get_string(sortby="s1_table_name", reversesort=True))
        print(y.get_string())



    def clean(self):
        sql1 = "drop table if exists found_values;"
        sql2 = "drop table if exists tchecksum;"
        sql3 = "drop table if exists s1_values;"
        sql4 = "drop table if exists s2_values;"
        sql5 = "drop table if exists noprimary_table;"
        self.cursor.execute(sql1)
        self.cursor.execute(sql2)
        self.cursor.execute(sql3)
        self.cursor.execute(sql4)
        self.cursor.execute(sql5)

        print('+------------end---------------+')
        print('{} clean 5 table ok'.format(self.host))
      

    def __del__(self):        
        self.cursor.close()
        self.conn.close()


if __name__ == "__main__":
    db1=dbclass('x.x.x.x',3306,'root','123456','employees')
    db1.intdb()
    db1.get_checksum()

    db2=dbclass('x.x.x.x',3306,'root','123456','employees')
    db2.intdb()
    db2.get_checksum()

    db1_result = db1.get_result()
    db2_result = db2.get_result()

    db1.cmp_result(db1_result,db2_result) 

    db1.clean()
    db2.clean()
    print('\n')
