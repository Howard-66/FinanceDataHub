"""
Pytest配置文件

确保在每个测试模块开始前导入providers，触发装饰器注册。
"""

import sys


def pytest_configure(config):
    """在pytest配置阶段确保providers模块被导入"""
    # 确保providers模块被导入，触发装饰器注册
    if "finance_data_hub.providers" not in sys.modules:
        import finance_data_hub.providers  # noqa: F401
