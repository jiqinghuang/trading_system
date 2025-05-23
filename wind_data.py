# -*- coding: utf-8 -*-
"""
Wind金融数据获取与存储模块(Parquet版本)

该模块提供从Wind金融终端获取贵金属(T+D)市场数据，
并存储到本地Parquet文件的功能。支持多品种数据管理。

主要功能：
- 从Wind获取AG(T+D)和AU(T+D)的OHLC等市场数据
- 数据存储到本地Parquet文件
- 支持增量更新数据
- 支持多品种并行处理
"""

from datetime import datetime, timedelta
import os
import re
import polars as pl
from WindPy import w

# 配置参数
SYMBOLS = {
    "AFI.WI", "AGFI.WI", "ALFI.WI", "AOFI.WI", "APLFI.WI",
    "AUFI.WI", "BCFI.WI", "BFI.WI", "BRFI.WI", "BUFI.WI",
    "CFFI.WI", "CFI.WI", "CJFI.WI", "CSFI.WI", "CUFI.WI",
    "CYFI.WI", "EBFI.WI", "ECFI.WI", "EGFI.WI", "FBFI.WI",
    "FGFI.WI", "FUFI.WI", "HCFI.WI", "IFI.WI", "JDFI.WI",
    "JFI.WI", "JMFI.WI", "LCFI.WI", "LFI.WI", "LGFI.WI",
    "LHFI.WI", "LUFI.WI", "MAFI.WI", "MFI.WI", "NIFI.WI",
    "NRFI.WI", "OIFI.WI", "PBFI.WI", "PFFI.WI", "PFI.WI",
    "PGFI.WI", "PKFI.WI", "PPFI.WI", "PRFI.WI", "PSFI.WI",
    "PXFI.WI", "RBFI.WI", "RMFI.WI", "RRFI.WI", "RSFI.WI",
    "RUFI.WI", "SAFI.WI", "SCFI.WI", "SFFI.WI", "SHFI.WI",
    "SIFI.WI", "SMFI.WI", "SNFI.WI", "SPFI.WI", "SRFI.WI",
    "SSFI.WI", "TAFI.WI", "URFI.WI", "VFI.WI", "WRFI.WI",
    "YFI.WI", "ZNFI.WI", "AU(T+D).SGE", "AG(T+D).SGE"
    }  # 需要获取的品种列表

DATA_DIR = os.path.join(os.path.dirname(__file__), "data")  # 数据存储目录
DEFAULT_START_DATE = datetime(1990, 1, 1).date()  # 默认起始日期

def ensure_data_dir():
    """确保数据存储目录存在，如不存在则创建"""
    os.makedirs(DATA_DIR, exist_ok=True)

def get_parquet_path(symbol):
    """获取Parquet文件路径"""
    clean_symbol = re.sub(r'[^\w]', '_', symbol)
    return os.path.join(DATA_DIR, f"{clean_symbol}.parquet")

def fetch_wind_data(symbol, start_date, end_date):
    """从Wind API获取指定品种的市场数据"""
    print(f"正在获取 {symbol} 数据({start_date} 至 {end_date})...")
    data = w.wsd(
        symbol,
        "open,high,low,close,settle,volume,oi,amt",
        start_date.strftime("%Y-%m-%d"),
        end_date.strftime("%Y-%m-%d"),
        "unit=1"
    )
    if data.ErrorCode != 0:
        print(f"警告: {symbol} 数据获取错误 - {data.Data[0][0]}")
        return None
    
    return pl.DataFrame({
        "date": [d.strftime('%Y-%m-%d') for d in data.Times],
        **{field.lower(): values for field, values in zip(data.Fields, data.Data)}
    })

def get_last_date(symbol):
    """从Parquet文件中获取最后交易日"""
    parquet_path = get_parquet_path(symbol)
    if os.path.exists(parquet_path):
        df = pl.read_parquet(parquet_path)
        return datetime.strptime(df["date"].max(), '%Y-%m-%d').date()
    return None

def preprocess_dataframe(df):
    """预处理数据框，删除除date列外全为NULL或NaN的行"""
    value_cols = [col for col in df.columns if col != 'date']
    if not value_cols:
        return df 
    # 检查这些列是否全部为NULL或NaN
    null_or_nan_condition = pl.all_horizontal([
        (pl.col(col).is_null() | pl.col(col).is_nan()) for col in value_cols
    ])
    return df.filter(~null_or_nan_condition)

def update_symbol_data(symbol, end_date):
    """更新指定品种的市场数据(增量版本)"""
    if not isinstance(end_date, datetime) and not str(type(end_date)) == "<class 'datetime.date'>":
        raise ValueError("end_date必须是日期类型")
        
    last_date = get_last_date(symbol)
    start_date = last_date + timedelta(days=1) if last_date else DEFAULT_START_DATE
    parquet_path = get_parquet_path(symbol)
    
    if last_date and end_date <= last_date:
        print(f"{symbol} 数据已是最新(已有数据至{last_date})")
        return
    
    new_data = fetch_wind_data(symbol, start_date, end_date)
    # 修改类型转换逻辑，添加错误处理
    new_data = new_data.with_columns([
        pl.col(col).cast(pl.Float64, strict=False).alias(col)
        for col in ['open', 'high', 'low', 'close', 'settle', 'volume', 'oi', 'amt']
    ])
    
    new_data = preprocess_dataframe(new_data)
    
    if os.path.exists(parquet_path):
        existing_data = pl.read_parquet(parquet_path)
        combined_data = pl.concat([existing_data, new_data]) \
                        .unique("date") \
                        .sort("date")
    else:
        combined_data = new_data

    combined_data.write_parquet(parquet_path)
    print(f"{symbol} 数据已成功更新至 {end_date}")

def pre_update_validation(symbol):
    """更新前校验最后一行数据完整性"""
    parquet_path = get_parquet_path(symbol)
    if not os.path.exists(parquet_path):
        return
    
    df = pl.read_parquet(parquet_path)
    if df.is_empty():
        return
    # 只检查数值列
    numeric_cols = ['open','high','low','close','settle','volume','oi','amt']
    # 转换数值列类型
    df = df.with_columns([
        pl.col(col).cast(pl.Float64, strict=False)
        for col in numeric_cols
    ])
    
    last_row = df.tail(1)
    # 只检查数值列是否有空值
    has_invalid = last_row.select(
        pl.any_horizontal([pl.col(col).is_null() | pl.col(col).is_nan() for col in numeric_cols])
    ).item()
    
    if has_invalid:
        print(f"发现{symbol}最后交易日数据不完整，正在清理...")
        cleaned_df = df.head(-1)
        cleaned_df.write_parquet(parquet_path)
        new_last_date = cleaned_df["date"].max()
        print(f"已清理空值，最新有效数据日期：{new_last_date}")
    else:
        print(f"{symbol} 最新数据校验通过")

def validate_date_input(date_str):
    """验证日期输入格式"""
    try:
        return datetime.strptime(date_str, "%Y%m%d").date()
    except ValueError:
        return None

def main(end_date):
    """主执行函数"""
    if not w.start():
        print("WindPy启动失败")
        return
    if not w.isconnected():
        print("WindPy连接未建立")
        return
        
    ensure_data_dir()
    # 先执行数据校验
    for symbol in SYMBOLS:
        pre_update_validation(symbol)            
    # 再执行数据更新
    for symbol in SYMBOLS:
        update_symbol_data(symbol, end_date)
        
    w.stop()


if __name__ == "__main__":
    while True:
        date_input = input("请输入截止日期(格式:YYYYMMDD): ")
        recent_date = validate_date_input(date_input)
        if recent_date:
            break
        print("日期格式错误，请重新输入(示例:20250509)")    
    main(end_date=recent_date)
