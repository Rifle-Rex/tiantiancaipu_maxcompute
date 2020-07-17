# coding=utf-8
from collections import defaultdict
import pymysql
import decimal
import json
import copy
import sys


class DecimalEncoder(json.JSONEncoder):

    def default(self, o):

        if isinstance(o, decimal.Decimal):
            return float(o)

        super(DecimalEncoder, self).default(o)

"""
mysql_conf={
    'host':'192.168.1.169',
    'user':'root',
    'password':'123456',
    'charset':'utf8mb4',
    'database':'tiantiancaipu',
}
"""
mysql_conf = {
    'host':'rm-2zeq1451z46w6mb59o.mysql.rds.aliyuncs.com',
    'user':'buydeem_ttcp',
    'password':'a2EVB66dgtX1dkof',
    'charset':'utf8mb4',
    'database':'buydeem_tiantiancaipu',
}


try:
    conn = pymysql.connect(**mysql_conf)
    cursor = conn.cursor(cursor=pymysql.cursors.DictCursor)
except BaseException as e:
    print(e)
    sys.exit(1)
 
result = []

sql = 'select * from sp_rp_arc_type where typename = "recipes"'
cursor.execute(sql)
if not cursor or cursor.rowcount == 0:
    sys.exit(1)

arc_type = cursor.fetchone()

main_columns = arc_type['main_columns']
attr_columns = arc_type['attr_columns']

main_columns = json.loads(main_columns)
attr_columns = json.loads(attr_columns)

for key in attr_columns.keys():
    value = attr_columns[key]
    if key not in main_columns.keys():
        main_columns[key] = value

for column in main_columns:
    column_type = main_columns[column]['type']
    multi_select = main_columns[column]['multi_select'] if 'multi_select' in main_columns[column].keys() else False
    print(column,column_type,multi_select)
    if column_type == 'select':
        sql = 'select * from sp_rp_attr where type = 0 and channel = 0 and name = %s'
        cursor.execute(sql, column)
        if not cursor or cursor.rowcount == 0:
            sys.exit('属性"%s"不存在' % column)
        attr = cursor.fetchone()
        values = {}
        for value in attr['value'].split(','):
            values[value] = [value]
        sql = 'select * from sp_rp_attr_synonyms where type ="attr" and index_id = %s'
        cursor.execute(sql, attr['id'])
        if cursor and cursor.rowcount > 0:
            rows = cursor.fetchall()
            for row in rows:
                if row['attr_name'] in values.keys():
                    values[row['attr_name']] = values[row['attr_name']]+row['synonyms'].split(",")
            values[row['attr_name']] = sorted(values[row['attr_name']], key=lambda i:len(i) ,reverse=True)
        result.append(copy.copy({'column':column, 'type':column_type, 'multi_select':multi_select, 'values':values}))
    elif column_type == 'enum':
        sql = 'select * from sp_rp_enum_index where egroup = %s'
        cursor.execute(sql, column)
        if not cursor or cursor.rowcount != 1:
            sys.exit("枚举属性主表有问题. %s" % column)
        enum = cursor.fetchone()
        sql = 'select * from sp_rp_enum where egroup = %s'
        cursor.execute(sql, column)
        if not cursor or cursor.rowcount == 0:
            sys.exit('属性"%s"不存在' % column)
        enums = cursor.fetchall()
        values = []
        synonyms = {}
        sql = 'select * from sp_rp_attr_synonyms where type = "enum" and index_id = %s'

        cursor.execute(sql, enum['id'])
        if cursor and cursor.rowcount >0:
            rows = cursor.fetchall()
            for row in rows:
                synonyms[row['attr_name']] = row['synonyms'].split(',')
        for enums_one in enums:
            evalue = enums_one['evalue']
            ename = enums_one['ename']
            values_synonyms = synonyms[ename] if ename in synonyms.keys() else []
            values_synonyms.append(ename)
            values_synonyms = sorted(values_synonyms, key=lambda i:len(i) ,reverse=True)
            values.append({'evalue':evalue, 'ename':ename, 'synonyms':values_synonyms})
        values = sorted(values, key=lambda i:i['evalue'], reverse=True)
        result.append(copy.copy({'column':column, 'type':column_type, 'multi_select':multi_select, 'values':values}))



with open("attr.json", "w") as f:
    json.dump(result, f, cls=DecimalEncoder, ensure_ascii=False)

print("done")
cursor.close()
conn.close()
