# -*- coding: utf-8 -*-
# @Time    : 2023/8/24 14:01
# @Author  : LAN
# @File    : storage_top_domain.py
# @desc    : 第一步 保存顶级 域名跟 谷歌分析 媒体ID


import sys
import os
path = os.path.dirname((os.getcwd()))
# print(path)
sys.path.append(path)
from  config import mysql_conn

def read_domain():
    '''读取顶级域名'''
    path = os.path.dirname((os.getcwd()))
    path = os.path.join(path,'generate_data','domain_an.txt') # 获取域名@id d的路径
    # print(path)
    with open(path,'r',encoding='utf-8') as f:
        domains = f.readlines()
    data_list = []
    for i in domains: # 循环
        data = i.strip().split('@') # 分割

        data_list.append(data)
    return data_list
def storage_data(conn,cursor,data_list):
    '''保存域名、媒体id'''
    sql =  """ INSERT INTO domain_analyze (domain,media_id,create_time) VALUES (%s,%s,NOW())
                         ON DUPLICATE KEY UPDATE update_time=NOW();
                """
    storange_list = []
    for list_ in data_list:
        storange_list.append(list_)
        if len(storange_list)>=300:
            cursor.executemany(sql,storange_list)
            conn.commit()
            storange_list.clear()
            print('大于三百保存一次')
    cursor.executemany(sql, storange_list)
    conn.commit()
    print('执行结束最后一次保存')


def main():
    # 自定义方法 链接 mysql
    conn,cursor = mysql_conn()
    # 读取 域名
    data_list = read_domain()
    # 保存
    storage_data(conn,cursor,data_list)
    cursor.close()
    conn.close()


if __name__ == '__main__':
    main()

