# will change and rich later
import numpy as np

class TradingStrategyCore:
    """Core trading strategy implementation module.

    Contains the main strategy logic including signal generation and technical
    indicators calculation. Currently implements EWMA crossover strategy.

    Attributes:
        dates: Array of trading dates
        prices: Array of price data (open, close)
        strategy_type: Type of strategy (default: 'EWMA')
        processed_data: Dictionary containing all processed strategy data
    """
    def __init__(self, data_handler, strategy_type='EWMA', **kwargs):
        self.dates = data_handler.dates
        self.open_prices = data_handler.open  # 明确命名
        self.close_prices = data_handler.close
        self.strategy_type = strategy_type
        self.strategy_params = kwargs
        self.processed_data = None
        self.indicator_name = strategy_type
        # EWMA策略参数设置
        if strategy_type == 'EWMA':
            self.span = kwargs.get('span', 30)
            self.indicator_name = f'EWMA_{self.span}'
        elif strategy_type == 'EWMA_LONG_ONLY':
            self.span = kwargs.get('span', 30)
            self.indicator_name = f'EWMA_LONG_ONLY_{self.span}'

    def generate_signals(self):
        """策略信号生成入口"""
        # 构建策略方法名
        method_name = f'_generate_{self.strategy_type.lower()}_signals'
        # 检查方法是否存在
        if not hasattr(self, method_name):
            raise ValueError(f"不支持的策略类型: {self.strategy_type}")
        # 获取并执行策略方法
        strategy_method = getattr(self, method_name)
        return strategy_method()
    
    def _generate_ewma_signals(self):
        """EWMA策略信号生成(允许做空)"""
        close_prices = self.close_prices
        # 计算执行价格，先取开盘和收盘的平均价（可修改）
        execution_price = (self.open_prices + self.close_prices) / 2
        
        # 计算EWMA
        alpha = 2 / (self.span + 1)
        ewma = np.zeros_like(close_prices)
        ewma[0] = close_prices[0]
        for i in range(1, len(close_prices)):
            ewma[i] = alpha * close_prices[i] + (1 - alpha) * ewma[i-1]
        
        # 向量化信号生成
        prev_close = np.roll(close_prices, 1)
        prev_ewma = np.roll(ewma, 1)
        prev_close[0] = np.nan  # 第一个元素设为nan避免误判
        prev_ewma[0] = np.nan
        
        # 生成交易信号
        trading_signal = np.zeros_like(close_prices)
        trading_signal[(close_prices > ewma) & (prev_close < prev_ewma)] = 1 # 买入信号
        trading_signal[(close_prices < ewma) & (prev_close > prev_ewma)] = -1 # 卖出信号
        
        # 生成持仓信号(允许做空)和行动状态
        position = np.zeros_like(close_prices)
        n = close_prices.shape[0]  # 数据长度
        action_states = np.array(['hold']*n)  # 初始化行动状态        
        for i, signal in enumerate(trading_signal[:-1]):  # 少循环一次
            if signal == 1:  # 买入信号
                position[i+1] = 1
                action_states[i+1] = 'buy'
            elif signal == -1:  # 卖出信号
                position[i+1] = -1
                action_states[i+1] = 'sell'
            else:
                position[i+1] = position[i]  # 保持原有持仓
        
        # 生成回测与画图数据
        self.processed_data = {
            'Date': self.dates,
            'Close': close_prices,
            'ExecutionPrice': execution_price,  # 修改为ExecutionPrice
            self.indicator_name: ewma,
            'TradingSignal': trading_signal,
            'Position': position,
            'ActionStates': action_states  # 添加行动状态到数据中
        }
        return self.processed_data

    def _generate_ewma_long_only_signals(self):
        """EWMA策略信号生成(仅做多)"""
        close_prices = self.close_prices
        # 计算执行价格，先取开盘和收盘的平均价（可修改）
        execution_price = (self.open_prices + self.close_prices) / 2
        
        # 计算EWMA
        alpha = 2 / (self.span + 1)
        ewma = np.zeros_like(close_prices)
        ewma[0] = close_prices[0]
        for i in range(1, len(close_prices)):
            ewma[i] = alpha * close_prices[i] + (1 - alpha) * ewma[i-1]
        
        # 向量化信号生成
        prev_close = np.roll(close_prices, 1)
        prev_ewma = np.roll(ewma, 1)
        prev_close[0] = np.nan  # 第一个元素设为nan避免误判
        prev_ewma[0] = np.nan
        
        # 生成交易信号(保留买卖信号)
        trading_signal = np.zeros_like(close_prices)
        trading_signal[(close_prices > ewma) & (prev_close < prev_ewma)] = 1
        trading_signal[(close_prices < ewma) & (prev_close > prev_ewma)] = -1
        
        # 生成持仓信号(仅做多)和行动状态
        position = np.zeros_like(close_prices)
        n = close_prices.shape[0]  # 数据长度
        action_states = np.array(['hold']*n)  # 初始化行动状态
        for i, signal in enumerate(trading_signal[:-1]):  # 少循环一次
            if signal == 1:  # 买入信号
                position[i+1] = 1  # 隔日买入做多
                action_states[i+1] = 'buy'
            elif signal == -1:  # 卖出信号
                position[i+1] = 0  # 隔日卖出平仓
                action_states[i+1] = 'sell'
            else:
                position[i+1] = position[i]  # 保持原有持仓

      
        # 生成回测与画图数据
        self.processed_data = {
            'Date': self.dates,
            'Close': close_prices,
            'ExecutionPrice': execution_price,  # 修改为ExecutionPrice
            self.indicator_name: ewma,
            'TradingSignal': trading_signal,
            'Position': position,
            'ActionStates': action_states  # 添加行动状态到数据中
        }
        return self.processed_data
