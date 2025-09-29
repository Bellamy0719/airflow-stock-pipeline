import pandas as pd
import numpy as np


def test_rsi_range():
    s = pd.Series(np.linspace(100, 120, 60))  # 模拟价格序列
    delta = s.diff()
    gain = delta.where(delta > 0, 0.0)
    loss = -delta.where(delta < 0, 0.0)
    rs = gain.rolling(14).mean() / loss.rolling(14).mean()
    rsi = 100 - (100 / (1 + rs))

    assert (rsi.dropna() >= 0).all() and (rsi.dropna() <= 100).all()


def test_sma_correct():
    s = pd.Series([1, 2, 3, 4, 5])
    sma5 = s.rolling(5).mean()
    assert sma5.iloc[-1] == 3  # (1+2+3+4+5)/5


def test_golden_death_cross():
    df = pd.DataFrame({
        "sma_50": [10, 11, 11, 11, 13],  # 最后一行才 > 12
        "sma_200": [12, 12, 12, 12, 12],
    })
    golden_cross = (df["sma_50"] > df["sma_200"]) & (df["sma_50"].shift(1) <= df["sma_200"].shift(1))
    death_cross  = (df["sma_50"] < df["sma_200"]) & (df["sma_50"].shift(1) >= df["sma_200"].shift(1))

    assert golden_cross.iloc[-1] == True   # 最后一天触发黄金交叉
    assert death_cross.iloc[-1] == False

