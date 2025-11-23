"""
测试频率提取逻辑
"""

def test_frequency_extraction():
    """测试从data_type中提取频率"""
    test_cases = [
        ("minute_1", "1m"),
        ("minute_5", "5m"),
        ("minute_15", "15m"),
        ("minute_30", "30m"),
        ("minute_60", "60m"),
        ("minute", "1m"),  # 没有下划线的情况
    ]

    for data_type, expected_freq in test_cases:
        # 模拟代码逻辑
        if "_" in data_type:
            minute_freq = data_type.split("_")[1]
            freq = f"{minute_freq}m"
        else:
            freq = "1m"

        print(f"data_type: {data_type:15} -> freq: {freq:5} (expected: {expected_freq})")
        assert freq == expected_freq, f"Expected {expected_freq}, got {freq}"

    print("\n✅ All frequency extraction tests passed!")

if __name__ == "__main__":
    test_frequency_extraction()
