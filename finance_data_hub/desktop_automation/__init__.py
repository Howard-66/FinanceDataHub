"""Host-side desktop automation used by FinanceDataHub scheduled jobs."""

from .wind_excel import refresh_wind_workbook

__all__ = ["refresh_wind_workbook"]
