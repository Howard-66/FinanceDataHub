## 下载历史行情数据

download_history_data(stock_code, period, start_time='', end_time='', incrementally = None)
释义
补充历史行情数据
参数
stock_code - string 合约代码
period - string 周期
start_time - string 起始时间
end_time - string 结束时间
incrementally - 是否增量下载
bool - 是否增量下载
None - 使用start_time控制，start_time为空则增量下载，增量下载时会从本地最后一条数据往后下载
返回
无
备注
同步执行，补充数据完成后返回

---

download_history_data2(stock_list, period, start_time='', end_time='', callback=None,incrementally = None)
释义

补充历史行情数据，批量版本
参数

stock_list - list 合约列表

period - string 周期

start_time - string 起始时间

end_time - string 结束时间

callback - func 回调函数

参数为进度信息dict

total - 总下载个数
finished - 已完成个数
stockcode - 本地下载完成的合约代码
message - 本次信息

def on_progress(data):
	print(data)
	# {'finished': 1, 'total': 50, 'stockcode': '000001.SZ', 'message': ''}
返回

无
备注

同步执行，补充数据完成后返回
有任务完成时通过回调函数返回进度信息

---
## 获取行情数据

get_market_data(field_list=[], stock_list=[], period='1d', start_time='', end_time='', count=-1, dividend_type='none', fill_data=True)
释义
从缓存获取行情数据，是主动获取行情的主要接口
参数
field_list - list 数据字段列表，传空则为全部字段
stock_list - list 合约代码列表
period - string 周期
start_time - string 起始时间
end_time - string 结束时间
count - int 数据个数
默认参数，大于等于0时，若指定了start_time，end_time，此时以end_time为基准向前取count条；若start_time，end_time缺省，默认取本地数据最新的count条数据；若start_time，end_time，count都缺省时，默认取本地全部数据
dividend_type - string 除权方式
fill_data - bool 是否向后填充空缺数据
返回
period为1m 5m 1d等K线周期时
返回dict { field1 : value1, field2 : value2, ... }
field1, field2, ... ：数据字段
value1, value2, ... ：pd.DataFrame 数据集，index为stock_list，columns为time_list
各字段对应的DataFrame维度相同、索引相同
period为tick分笔周期时
返回dict { stock1 : value1, stock2 : value2, ... }
stock1, stock2, ... ：合约代码
value1, value2, ... ：np.ndarray 数据集，按数据时间戳time增序排列
备注
获取lv2数据时需要数据终端有lv2数据权限
时间范围为闭区间

---

## 获取本地行情数据

get_local_data(field_list=[], stock_list=[], period='1d', start_time='', end_time='', count=-1,
               dividend_type='none', fill_data=True, data_dir=data_dir)
释义
从本地数据文件获取行情数据，用于快速批量获取历史部分的行情数据
参数
field_list - list 数据字段列表，传空则为全部字段
stock_list - list 合约代码列表
period - string 周期
start_time - string 起始时间
end_time - string 结束时间
count - int 数据个数
dividend_type - string 除权方式
fill_data - bool 是否向后填充空缺数据
data_dir - string MiniQmt配套路径的userdata_mini路径，用于直接读取数据文件。默认情况下xtdata会通过连接向MiniQmt直接获取此路径，无需额外设置。如果需要调整，可以将数据路径作为data_dir传入，也可以直接修改xtdata.data_dir以改变默认值
返回
period为1m 5m 1dK线周期时
返回dict { field1 : value1, field2 : value2, ... }
field1, field2, ... ：数据字段
value1, value2, ... ：pd.DataFrame 数据集，index为stock_list，columns为time_list
各字段对应的DataFrame维度相同、索引相同
period为tick分笔周期时
返回dict { stock1 : value1, stock2 : value2, ... }
stock1, stock2, ... ：合约代码
value1, value2, ... ：np.ndarray 数据集，按数据时间戳time增序排列
备注
仅用于获取level1数据

---

## 下载节假日数据

download_holiday_data()
释义

下载节假日数据
参数

无
返回

无

---

## 获取交易日列表

get_trading_dates(market, start_time='', end_time='', count=-1)
释义
获取交易日列表
参数
market - string 市场代码
start_time - string 起始时间
end_time - string 结束时间
count - int 数据个数
返回
list 时间戳列表，[ date1, date2, ... ]
备注
无

---

## 获取最新节假日数据

get_holidays()
释义
获取截止到当年的节假日日期
参数
无
返回
list，为8位的日期字符串格式
备注
无

---

## 获取交易日历

get_trading_calendar(market, start_time = '', end_time = '')
释义
获取指定市场交易日历
参数
market - str 市场
start_time - str 起始时间，8位字符串。为空表示当前市场首个交易日时间
end_time - str 结束时间，8位字符串。为空表示当前时间
返回
返回list，完整的交易日列表
备注
结束时间可以填写未来时间，获取未来交易日。需要下载节假日列表

---

## 获取可用周期列表

get_period_list()
释义

返回可用周期列表
参数

无
返回

list 周期列表

---

## 下载财务数据

download_financial_data(stock_list, table_list=[])
释义
下载财务数据
参数
stock_list - list 合约代码列表
table_list - list 财务数据表名列表
返回
无
备注
同步执行，补充数据完成后返回

download_financial_data2(stock_list, table_list=[], start_time='', end_time='', callback=None)
释义

下载财务数据
参数

stock_list - list 合约代码列表

table_list - list 财务数据表名列表

start_time - string 起始时间

end_time - string 结束时间

以m_anntime披露日期字段，按[start_time, end_time]范围筛选
callback - func 回调函数

参数为进度信息dict

total - 总下载个数
finished - 已完成个数
stockcode - 本地下载完成的合约代码
message - 本次信息

def on_progress(data):
	print(data)
	# {'finished': 1, 'total': 50, 'stockcode': '000001.SZ', 'message': ''}
返回

无
备注

同步执行，补充数据完成后返回

---

## 获取财务数据

get_financial_data(stock_list, table_list=[], start_time='', end_time='', report_type='report_time')
释义

获取财务数据
参数

stock_list - list 合约代码列表

table_list - list 财务数据表名称列表


'Balance'          #资产负债表
'Income'           #利润表
'CashFlow'         #现金流量表
'Capital'          #股本表
'Holdernum'        #股东数
'Top10holder'      #十大股东
'Top10flowholder'  #十大流通股东
'Pershareindex'    #每股指标
start_time - string 起始时间

end_time - string 结束时间

report_type - string 报表筛选方式


'report_time' 	#截止日期
'announce_time' #披露日期
返回

dict 数据集 { stock1 : datas1, stock2 : data2, ... }
stock1, stock2, ... ：合约代码
datas1, datas2, ... ：dict 数据集 { table1 : table_data1, table2 : table_data2, ... }
table1, table2, ... ：财务数据表名
table_data1, table_data2, ... ：pd.DataFrame 数据集，数据字段详见附录 - 财务数据字段列表
备注

无

---

## 下载板块分类信息

download_sector_data()
释义
下载板块分类信息
参数
无
返回
无
备注
同步执行，下载完成后返回

---

## 获取板块列表

get_sector_list()
释义
获取板块列表
参数
无
返回
list 板块列表，[ sector1, sector2, ... ]
备注
需要下载板块分类信息

---

## 获取板块成分股列表

get_stock_list_in_sector(sector_name)
释义
获取板块成分股列表
参数
sector_name - string 版块名称
返回
list 成分股列表，[ stock1, stock2, ... ]
备注
需要板块分类信息

---

## 下载指数成分权重信息

download_index_weight()
释义
下载指数成分权重信息
参数
无
返回
无
备注
同步执行，下载完成后返回

---

## 获取指数成分权重信息

get_index_weight(index_code)
释义
获取指数成分权重信息
参数
index_code - string 指数代码
返回
dict 数据字典，{ stock1 : weight1, stock2 : weight2, ... }
备注
需要下载指数成分权重信息

---
