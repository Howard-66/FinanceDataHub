#!/usr/bin/env python3
"""
数据准确性验证脚本

比较连续聚合数据与手动 Pandas 重采样的结果，验证聚合逻辑的正确性。
支持单个或多个股票的时间范围验证。
"""

import asyncio
import argparse
import sys
from datetime import datetime
from typing import List, Tuple, Dict, Any
import pandas as pd
from pathlib import Path

# 添加项目根目录到 Python 路径
sys.path.insert(0, str(Path(__file__).parent.parent))

from finance_data_hub.config import get_settings
from finance_data_hub.database.manager import DatabaseManager
from finance_data_hub.database.operations import DataOperations
from finance_data_hub.sdk import FinanceDataHub


class AggregateValidator:
    """连续聚合数据验证器"""

    def __init__(self):
        """初始化验证器"""
        self.settings = get_settings()
        self.db_manager = DatabaseManager(self.settings)
        self.data_ops = DataOperations(self.db_manager)
        self.sdk = FinanceDataHub(self.settings)

    async def initialize(self):
        """初始化数据库连接"""
        await self.db_manager.initialize()

    async def close(self):
        """关闭连接"""
        await self.db_manager.close()

    async def validate_symbol_weekly(
        self,
        symbol: str,
        start_date: str,
        end_date: str,
        tolerance: float = 0.0001
    ) -> Dict[str, Any]:
        """
        验证 symbol_weekly 聚合的准确性

        Args:
            symbol: 股票代码
            start_date: 开始日期
            end_date: 结束日期
            tolerance: 允许误差（默认 0.01%）

        Returns:
            验证结果字典
        """
        print(f"\n正在验证 {symbol} 的周线数据聚合...")

        # 获取日线数据
        daily_query = """
            SELECT time, symbol, open, high, low, close, volume, amount, adj_factor
            FROM symbol_daily
            WHERE symbol = :symbol
            AND time BETWEEN :start_date AND :end_date
            ORDER BY time
        """

        from sqlalchemy import text
        async with self.db_manager._engine.begin() as conn:
            result = await conn.execute(
                text(daily_query),
                {"symbol": symbol, "start_date": start_date, "end_date": end_date}
            )
            daily_rows = result.fetchall()

        if not daily_rows:
            return {
                "symbol": symbol,
                "status": "FAILED",
                "error": "未找到日线数据"
            }

        # 转换为 DataFrame
        daily_data = pd.DataFrame([row._asdict() for row in daily_rows])
        daily_data.set_index('time', inplace=True)

        # 手动计算周聚合
        weekly_manual = self._calculate_manual_weekly(daily_data)

        # 获取聚合数据
        weekly_agg = await self.data_ops.get_weekly_data([symbol], start_date, end_date)

        if weekly_agg is None or len(weekly_agg) == 0:
            return {
                "symbol": symbol,
                "status": "FAILED",
                "error": "聚合数据为空"
            }

        # 比较数据
        comparison_results = self._compare_weekly_data(weekly_manual, weekly_agg, tolerance)

        return {
            "symbol": symbol,
            "status": "PASSED" if comparison_results["all_match"] else "FAILED",
            "total_weeks": len(weekly_agg),
            "matching_weeks": comparison_results["matching_count"],
            "failing_weeks": comparison_results["failing_count"],
            "max_error": comparison_results["max_error"],
            "details": comparison_results["details"]
        }

    async def validate_symbol_monthly(
        self,
        symbol: str,
        start_date: str,
        end_date: str,
        tolerance: float = 0.0001
    ) -> Dict[str, Any]:
        """
        验证 symbol_monthly 聚合的准确性

        Args:
            symbol: 股票代码
            start_date: 开始日期
            end_date: 结束日期
            tolerance: 允许误差（默认 0.01%）

        Returns:
            验证结果字典
        """
        print(f"\n正在验证 {symbol} 的月线数据聚合...")

        # 获取日线数据
        daily_query = """
            SELECT time, symbol, open, high, low, close, volume, amount, adj_factor
            FROM symbol_daily
            WHERE symbol = :symbol
            AND time BETWEEN :start_date AND :end_date
            ORDER BY time
        """

        from sqlalchemy import text
        async with self.db_manager._engine.begin() as conn:
            result = await conn.execute(
                text(daily_query),
                {"symbol": symbol, "start_date": start_date, "end_date": end_date}
            )
            daily_rows = result.fetchall()

        if not daily_rows:
            return {
                "symbol": symbol,
                "status": "FAILED",
                "error": "未找到日线数据"
            }

        # 转换为 DataFrame
        daily_data = pd.DataFrame([row._asdict() for row in daily_rows])
        daily_data.set_index('time', inplace=True)

        # 手动计算月聚合
        monthly_manual = self._calculate_manual_monthly(daily_data)

        # 获取聚合数据
        monthly_agg = await self.data_ops.get_monthly_data([symbol], start_date, end_date)

        if monthly_agg is None or len(monthly_agg) == 0:
            return {
                "symbol": symbol,
                "status": "FAILED",
                "error": "聚合数据为空"
            }

        # 比较数据
        comparison_results = self._compare_monthly_data(monthly_manual, monthly_agg, tolerance)

        return {
            "symbol": symbol,
            "status": "PASSED" if comparison_results["all_match"] else "FAILED",
            "total_months": len(monthly_agg),
            "matching_months": comparison_results["matching_count"],
            "failing_months": comparison_results["failing_count"],
            "max_error": comparison_results["max_error"],
            "details": comparison_results["details"]
        }

    def _calculate_manual_weekly(self, daily_data: pd.DataFrame) -> pd.DataFrame:
        """手动计算周聚合"""
        # 按周聚合
        weekly = daily_data.resample('W').agg({
            'open': 'first',
            'high': 'max',
            'low': 'min',
            'close': 'last',
            'volume': 'sum',
            'amount': 'sum',
            'adj_factor': 'last',
            'symbol': 'first'
        }).reset_index()

        # 应用复权因子调整
        if len(weekly) > 0:
            weekly['first_adj'] = weekly.groupby(weekly.index // 1)['adj_factor'].transform('first')
            weekly['last_adj'] = weekly['adj_factor']

            # 复权价格计算
            weekly['open'] = weekly['open'] * weekly['last_adj'] / weekly['first_adj']
            weekly['high'] = weekly['high'] * weekly['last_adj'] / weekly['first_adj']
            weekly['low'] = weekly['low'] * weekly['last_adj'] / weekly['first_adj']

        return weekly

    def _calculate_manual_monthly(self, daily_data: pd.DataFrame) -> pd.DataFrame:
        """手动计算月聚合"""
        # 按月聚合
        monthly = daily_data.resample('M').agg({
            'open': 'first',
            'high': 'max',
            'low': 'min',
            'close': 'last',
            'volume': 'sum',
            'amount': 'sum',
            'adj_factor': 'last',
            'symbol': 'first'
        }).reset_index()

        # 应用复权因子调整
        if len(monthly) > 0:
            monthly['first_adj'] = monthly.groupby(monthly.index // 1)['adj_factor'].transform('first')
            monthly['last_adj'] = monthly['adj_factor']

            # 复权价格计算
            monthly['open'] = monthly['open'] * monthly['last_adj'] / monthly['first_adj']
            monthly['high'] = monthly['high'] * monthly['last_adj'] / monthly['first_adj']
            monthly['low'] = monthly['low'] * monthly['last_adj'] / monthly['first_adj']

        return monthly

    def _compare_weekly_data(
        self,
        manual_data: pd.DataFrame,
        agg_data: pd.DataFrame,
        tolerance: float
    ) -> Dict[str, Any]:
        """比较周聚合数据"""
        if len(manual_data) == 0 or len(agg_data) == 0:
            return {
                "all_match": False,
                "matching_count": 0,
                "failing_count": 0,
                "max_error": 0.0,
                "details": ["数据为空"]
            }

        details = []
        max_error = 0.0
        matching_count = 0
        failing_count = 0

        # 比较每一周
        for idx in range(min(len(manual_data), len(agg_data))):
            manual_row = manual_data.iloc[idx]
            agg_row = agg_data.iloc[idx]

            week_errors = []

            # 比较价格字段（允许误差）
            price_fields = ['open', 'high', 'low', 'close']
            for field in price_fields:
                manual_val = float(manual_row[field])
                agg_val = float(agg_row[field])

                if manual_val != 0:
                    error = abs(agg_val - manual_val) / manual_val
                    max_error = max(max_error, error)
                    week_errors.append(f"{field}: {error:.6f}")

                    if error <= tolerance:
                        matching_count += 1
                    else:
                        failing_count += 1

            if week_errors:
                details.append({
                    "period": idx + 1,
                    "errors": week_errors,
                    "failed": any(e > tolerance for e in [float(e.split(': ')[1]) for e in week_errors])
                })

        all_match = failing_count == 0

        return {
            "all_match": all_match,
            "matching_count": matching_count,
            "failing_count": failing_count,
            "max_error": max_error,
            "details": details
        }

    def _compare_monthly_data(
        self,
        manual_data: pd.DataFrame,
        agg_data: pd.DataFrame,
        tolerance: float
    ) -> Dict[str, Any]:
        """比较月聚合数据"""
        if len(manual_data) == 0 or len(agg_data) == 0:
            return {
                "all_match": False,
                "matching_count": 0,
                "failing_count": 0,
                "max_error": 0.0,
                "details": ["数据为空"]
            }

        details = []
        max_error = 0.0
        matching_count = 0
        failing_count = 0

        # 比较每一月
        for idx in range(min(len(manual_data), len(agg_data))):
            manual_row = manual_data.iloc[idx]
            agg_row = agg_data.iloc[idx]

            month_errors = []

            # 比较价格字段（允许误差）
            price_fields = ['open', 'high', 'low', 'close']
            for field in price_fields:
                manual_val = float(manual_row[field])
                agg_val = float(agg_row[field])

                if manual_val != 0:
                    error = abs(agg_val - manual_val) / manual_val
                    max_error = max(max_error, error)
                    month_errors.append(f"{field}: {error:.6f}")

                    if error <= tolerance:
                        matching_count += 1
                    else:
                        failing_count += 1

            if month_errors:
                details.append({
                    "period": idx + 1,
                    "errors": month_errors,
                    "failed": any(e > tolerance for e in [float(e.split(': ')[1]) for e in month_errors])
                })

        all_match = failing_count == 0

        return {
            "all_match": all_match,
            "matching_count": matching_count,
            "failing_count": failing_count,
            "max_error": max_error,
            "details": details
        }

    async def validate_all_aggregates(
        self,
        symbols: List[str],
        start_date: str,
        end_date: str,
        tolerance: float = 0.0001
    ) -> Dict[str, Any]:
        """
        验证所有聚合类型的准确性

        Args:
            symbols: 股票代码列表
            start_date: 开始日期
            end_date: 结束日期
            tolerance: 允许误差

        Returns:
            验证结果汇总
        """
        results = {
            "summary": {
                "total_symbols": len(symbols),
                "passed": 0,
                "failed": 0
            },
            "weekly": {},
            "monthly": {},
            "errors": []
        }

        for symbol in symbols:
            print(f"\n{'='*60}")
            print(f"验证股票: {symbol}")
            print(f"{'='*60}")

            try:
                # 验证周线
                weekly_result = await self.validate_symbol_weekly(
                    symbol, start_date, end_date, tolerance
                )
                results["weekly"][symbol] = weekly_result

                if weekly_result["status"] == "PASSED":
                    results["summary"]["passed"] += 1
                    print(f"✅ {symbol} 周线数据验证通过")
                else:
                    results["summary"]["failed"] += 1
                    print(f"❌ {symbol} 周线数据验证失败: {weekly_result.get('error', '')}")

                # 验证月线
                monthly_result = await self.validate_symbol_monthly(
                    symbol, start_date, end_date, tolerance
                )
                results["monthly"][symbol] = monthly_result

                if monthly_result["status"] == "PASSED":
                    print(f"✅ {symbol} 月线数据验证通过")
                else:
                    print(f"❌ {symbol} 月线数据验证失败: {monthly_result.get('error', '')}")

            except Exception as e:
                error_msg = f"验证 {symbol} 时出错: {str(e)}"
                print(f"❌ {error_msg}")
                results["errors"].append(error_msg)

        return results


def print_validation_report(results: Dict[str, Any]):
    """打印验证报告"""
    print(f"\n{'='*60}")
    print("验证报告汇总")
    print(f"{'='*60}")

    summary = results["summary"]
    print(f"\n总验证股票数量: {summary['total_symbols']}")
    print(f"通过验证: {summary['passed']}")
    print(f"失败验证: {summary['failed']}")

    if results["errors"]:
        print(f"\n错误列表:")
        for error in results["errors"]:
            print(f"  - {error}")

    # 详细结果
    print(f"\n{'='*60}")
    print("详细验证结果")
    print(f"{'='*60}")

    for symbol in results["weekly"].keys():
        weekly_result = results["weekly"][symbol]
        monthly_result = results["monthly"][symbol]

        print(f"\n股票: {symbol}")
        print(f"  周线: {weekly_result['status']} (周数: {weekly_result.get('total_weeks', 0)})")
        print(f"  月线: {monthly_result['status']} (月数: {monthly_result.get('total_months', 0)})")

        if weekly_result.get('max_error'):
            print(f"  周线最大误差: {weekly_result['max_error']:.8f}")
        if monthly_result.get('max_error'):
            print(f"  月线最大误差: {monthly_result['max_error']:.8f}")


async def main():
    """主函数"""
    parser = argparse.ArgumentParser(
        description="验证连续聚合数据的准确性",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例:
  python validate_aggregates.py --symbol 600519.SH --start 2024-01-01 --end 2024-12-31
  python validate_aggregates.py --symbols 600519.SH,000858.SZ --tolerance 0.0001
  python validate_aggregates.py --symbol 600519.SH --year 2024
        """
    )

    # 股票参数
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument(
        "--symbol",
        help="单个股票代码 (例如: 600519.SH)"
    )
    group.add_argument(
        "--symbols",
        help="多个股票代码，用逗号分隔 (例如: 600519.SH,000858.SZ)"
    )

    # 日期参数
    date_group = parser.add_mutually_exclusive_group()
    date_group.add_argument(
        "--start",
        help="开始日期 (YYYY-MM-DD)"
    )
    date_group.add_argument(
        "--year",
        help="验证整年数据 (例如: 2024)"
    )

    parser.add_argument(
        "--end",
        help="结束日期 (YYYY-MM-DD)，默认为今天"
    )

    parser.add_argument(
        "--tolerance",
        type=float,
        default=0.0001,
        help="允许误差（默认 0.0001 = 0.01%）"
    )

    args = parser.parse_args()

    # 处理日期
    if args.year:
        start_date = f"{args.year}-01-01"
        end_date = f"{args.year}-12-31"
    elif args.start and args.end:
        start_date = args.start
        end_date = args.end
    elif args.start:
        start_date = args.start
        end_date = datetime.now().strftime("%Y-%m-%d")
    else:
        end_date = datetime.now().strftime("%Y-%m-%d")
        start_date = "2024-01-01"

    # 处理股票代码
    if args.symbol:
        symbols = [args.symbol]
    else:
        symbols = [s.strip() for s in args.symbols.split(',')]

    print("连续聚合数据准确性验证")
    print("=" * 60)
    print(f"股票代码: {', '.join(symbols)}")
    print(f"日期范围: {start_date} 到 {end_date}")
    print(f"允许误差: {args.tolerance:.6f} ({args.tolerance*100:.4f}%)")

    # 创建验证器
    validator = AggregateValidator()

    try:
        await validator.initialize()

        # 执行验证
        results = await validator.validate_all_aggregates(
            symbols, start_date, end_date, args.tolerance
        )

        # 打印报告
        print_validation_report(results)

        # 返回退出码
        if results["summary"]["failed"] > 0:
            print(f"\n❌ 验证完成：{results['summary']['failed']} 个验证失败")
            sys.exit(1)
        else:
            print(f"\n✅ 所有验证通过！")
            sys.exit(0)

    except Exception as e:
        print(f"\n❌ 验证过程出错: {str(e)}")
        import traceback
        traceback.print_exc()
        sys.exit(2)
    finally:
        await validator.close()


if __name__ == "__main__":
    asyncio.run(main())
