import requests
import json
import pandas as pd

API_URL = "http://127.0.0.1:8100"  # 替换为 Windows 机器的实际 IP

# 1. download_history_data 示例
resp = requests.post(f"{API_URL}/download_history_data", json={
    "stock_code": "000001.SZ",
    "period": "1d",
    "start_time": "",
    "end_time": ""
})
print("download_history_data:", resp.json())

# 2. download_history_data2 示例
resp = requests.post(f"{API_URL}/download_history_data2", json={
    "stock_list": ["000001.SZ", "600000.SH"],
    "period": "1d",
    "start_time": "20230101",
    "end_time": "20231231"
})
print("download_history_data2:", resp.json())

# 3. get_market_data 示例
resp = requests.post(f"{API_URL}/get_market_data", json={
    "field_list": ["open", "close", "volume"],
    "stock_list": ["000001.SZ"],
    "period": "1d",
    "start_time": "20230101",
    "end_time": "20231231"
})
print("get_market_data:", json.dumps(resp.json(), ensure_ascii=False, indent=2))

# 4. get_local_data 示例
resp = requests.post(f"{API_URL}/get_local_data", json={
    "field_list": ["open", "close"],
    "stock_list": ["000001.SZ"],
    "period": "1d",
    "start_time": "20230101",
    "end_time": "20231231",
    "use_client_data": False
})
print("get_local_data:", json.dumps(resp.json(), ensure_ascii=False, indent=2))

# 5. download_sector_data 示例
# resp = requests.post(f"{API_URL}/download_sector_data")
# print("download_sector_data:", resp.json())

# 6. get_sector_list 示例
resp = requests.post(f"{API_URL}/get_sector_list")
print("get_sector_list:", json.dumps(resp.json(), ensure_ascii=False, indent=2))

# 7. download_holiday_data 示例
# resp = requests.post(f"{API_URL}/download_holiday_data")
# print("download_holiday_data:", resp.json())

# 8. get_holidays 示例
# resp = requests.post(f"{API_URL}/get_holidays")
# print("get_holidays:", resp.json())

# 9. get_trading_dates 示例
resp = requests.post(f"{API_URL}/get_trading_dates", json={
    "market": "SH",
    "start_time": "20230101",
    "end_time": "20231231",
    "count": -1
})
print("get_trading_dates:", json.dumps(resp.json(), ensure_ascii=False, indent=2))

# 10. download_financial_data 示例
# resp = requests.post(f"{API_URL}/download_financial_data", json={
#     "stock_list": ["601398.SH"],
#     "table_list": ['ASHARECASHFLOW']
# })
# print("download_financial_data:", json.dumps(resp.json(), ensure_ascii=False, indent=2))

# 11. download_financial_data2 示例
# resp = requests.post(f"{API_URL}/download_financial_data2", json={
#     "stock_list": ["601398.SH"],
#     "table_list": ['ASHARECASHFLOW'],
#     "start_time": "20240101",
#     "end_time": "20241231",
#     "callback": None
# })
# print("download_financial_data2:", json.dumps(resp.json(), ensure_ascii=False, indent=2))

# 10. get_financial_data 示例
resp = requests.post(f"{API_URL}/get_financial_data", json={
    "stock_list": ["601398.SH"],
    "table_list": ['ASHARECASHFLOW'],
    "start_time": "20240101",
    "end_time": "20241231",
    "report_type": "report_time"
})
print("get_financial_data:", json.dumps(resp.json(), ensure_ascii=False, indent=2))

# 11. get_stock_list_in_sector 示例
resp = requests.post(f"{API_URL}/get_stock_list_in_sector", json={
    "sector_name": "沪深300"
})
print("get_stock_list_in_sector:", resp.json()) 

# 12. get_divid_factors 示例
resp = requests.post(f"{API_URL}/get_divid_factors", json={
    "stock_code": "601398.SH",
    "start_time": "20240101",
    "end_time": "20241231"
})
print("get_divid_factors:", resp.json())

# 12. get_instrument_detail 示例
# resp = requests.post(f"{API_URL}/get_instrument_detail", json={
#     "stock_code": "601398.SH"
# })
# print("get_instrument_detail:", json.dumps(resp.json(), ensure_ascii=False, indent=2))

# 13. get_main_contract 示例
# resp = requests.post(f"{API_URL}/get_main_contract", json={
#     "stock_code": "601398.SH"
# })
# print("get_main_contract:", json.dumps(resp.json(), ensure_ascii=False, indent=2))