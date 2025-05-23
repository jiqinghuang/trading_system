from datetime import datetime
import numpy as np
import matplotlib.pyplot as plt
from tqdm import tqdm
from data_handler import DataHandler

def l1_trend_filter(y, lambda_, max_iter=100000, tol=1e-3):
    """
    使用ADMM算法实现L1趋势滤波
    参数:
        y: 输入信号
        lambda_: 正则化参数
        max_iter: 最大迭代次数
        tol: 收敛容差
    返回:
        滤波后的趋势信号
    """
    n = len(y)
    D = np.zeros((n-1, n))
    np.fill_diagonal(D, -1)
    np.fill_diagonal(D[:,1:], 1)
    x = y.copy()
    z = np.zeros(n-1)
    u = np.zeros(n-1)
    I = np.eye(n)
    inv = np.linalg.inv(I + D.T @ D)
    for _ in tqdm(range(max_iter)):
        # x更新
        x_new = inv @ (y + D.T @ (z - u))
        # z更新
        Dx = D @ x_new
        z_new = np.sign(Dx + u) * np.maximum(np.abs(Dx + u) - lambda_, 0)
        # 检查收敛
        primal_res = np.linalg.norm(Dx - z_new)
        dual_res = np.linalg.norm(-D.T @ (z_new - z))
        if primal_res < tol and dual_res < tol:
            break
        # u更新
        u += Dx - z_new
        x, z = x_new, z_new
    return x

# 示例使用
if __name__ == "__main__":
    # 设置中文字体
    plt.rcParams['font.sans-serif'] = ['SimHei']  # 用来正常显示中文标签
    plt.rcParams['axes.unicode_minus'] = False  # 用来正常显示负号    
    # 设置lambda参数
    lambda1 = 20
    lambda2 = 70
    # 使用data_handler读取数据
    data_handler = DataHandler("data/AG_T_D__SGE.parquet", file_type='parquet')
    data_handler.preprocess_data(
        start_date=datetime(2024,1,1), 
        end_date=datetime(2025,5,9)
    )
    close = data_handler.close
    log_close = np.log(close)
    t = data_handler.dates
    trend_admm1 = l1_trend_filter(log_close, lambda_=lambda1)
    trend_admm2 = l1_trend_filter(log_close, lambda_=lambda2)
    plt.figure(figsize=(14, 7))
    plt.plot(t, log_close, label='原始信号')
    plt.plot(t, trend_admm1, label=f'ADMM L1滤波 lambda={lambda1}', linewidth=3)
    plt.plot(t, trend_admm2, label=f'ADMM L1滤波 lambda={lambda2}', linewidth=3)
    plt.legend()
    plt.title(f'L1趋势滤波比较 (lambda1={lambda1}, lambda2={lambda2})')
    plt.show()
