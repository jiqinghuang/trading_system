# -*- coding: utf-8 -*-
"""
Created on Fri May 30 09:36:14 2025

@author: dell
"""

import pandas as pd
import sqlite3

# 读取Excel数据
member_name_387 = pd.read_excel('387家.xlsx', sheet_name='Sheet1', dtype={'客户编码': str, '席位编码': str})
member_name_185 = pd.read_excel('185家.xlsx', sheet_name='Sheet1', dtype={'客户编码': str, '席位编码': str})
monitor_data = pd.read_excel('异常监控记录.xlsx', sheet_name='Sheet1', dtype={'席位号': str, '客户编码': str, '交易日': str})

# 创建SQLite数据库连接
conn = sqlite3.connect('程序化异常交易.db')

# 将DataFrame存入数据库
member_name_387.to_sql('member_387', conn, if_exists='replace', index=False)
member_name_185.to_sql('member_185', conn, if_exists='replace', index=False)
monitor_data.to_sql('monitor_data', conn, if_exists='replace', index=False)

# 关闭连接
conn.close()

invalid_member_387 = member_name_387[~member_name_387['客户编码'].str.match(r'^\d{10}$', na=False)]
if not invalid_member_387.empty:
    print("member_name_387中不符合10位数字字符串的客户编码行:")
    print(invalid_member_387)

invalid_member_185 = member_name_185[~member_name_185['客户编码'].str.match(r'^\d{10}$', na=False)]
if not invalid_member_185.empty:
    print("member_name_185中不符合10位数字字符串的客户编码行:")
    print(invalid_member_185)

invalid_monitor = monitor_data[~monitor_data['客户编码'].str.match(r'^\d{10}$', na=False)]
if not invalid_monitor.empty:
    print("monitor_data中不符合10位数字字符串的客户编码行:")
    print(invalid_monitor)

missing_in_387 = member_name_185[~member_name_185['客户编码'].isin(member_name_387['客户编码'])]
if not missing_in_387.empty:
    print("member_name_185中不在member_name_387的客户编码行:")
    print(missing_in_387)
else:
    print("member_name_185中的所有客户编码都在member_name_387中")


# 新增列标记客户编码分类
monitor_data['是否程序化报备'] = '非程序化标记客户'  # 默认值
monitor_data.loc[monitor_data['客户编码'].isin(member_name_185['客户编码']), '是否程序化报备'] = '在185中'
monitor_data.loc[~monitor_data['客户编码'].isin(member_name_185['客户编码']) & 
                monitor_data['客户编码'].isin(member_name_387['客户编码']), '是否程序化报备'] = '在387不在185中'

monitor_data['交易日'] = pd.to_datetime(monitor_data['交易日']).dt.strftime('%Y-%m-%d')
# 保存结果，设置日期格式为仅日期
monitor_data.to_excel('标记后的异常监控记录.xlsx', index=False)
