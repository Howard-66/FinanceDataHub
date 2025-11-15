import os
import sys
import logging
from fastapi import FastAPI
from pydantic import BaseModel
from typing import List, Optional
from fastapi.responses import JSONResponse
import uvicorn
import traceback
from contextlib import asynccontextmanager
import numpy as np
from dotenv import load_dotenv

load_dotenv()

# 假设 xtquant 已经正确安装并可用
try:
    import xtquant.xtdata as xtdata
except ImportError:
    xtdata = None  # 方便本地调试

client_data_dir = os.getenv('CLIENT_DATA_DIR', None)

# 日志配置
os.makedirs('logs', exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(message)s',
    handlers=[
        logging.FileHandler('logs/api_service.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

@asynccontextmanager
async def lifespan(app):
    logging.info("正在尝试连接到QMT...")
    try:
        xtdata.connect()
        # xtdata.data_dir = client_data_dir
        logging.info("成功连接到QMT！")
    except Exception as e:
        logging.error(f"连接QMT失败: {e}")
    yield  # 这里可以加清理代码

app = FastAPI(title="xtquant 数据服务 API", lifespan=lifespan)

# --- Pydantic 请求模型 ---
class DownloadHistoryDataRequest(BaseModel):
    stock_code: str
    period: str
    start_time: Optional[str] = ''
    end_time: Optional[str] = ''
    incrementally: Optional[bool] = None

class DownloadHistoryData2Request(BaseModel):
    stock_list: List[str]
    period: str
    start_time: Optional[str] = ''
    end_time: Optional[str] = ''
    incrementally: Optional[bool] = None

class GetMarketDataRequest(BaseModel):
    field_list: Optional[List[str]] = []
    stock_list: List[str]
    period: Optional[str] = '1d'
    start_time: Optional[str] = ''
    end_time: Optional[str] = ''
    count: Optional[int] = -1
    dividend_type: Optional[str] = 'none'
    fill_data: Optional[bool] = True

class GetLocalDataRequest(BaseModel):
    field_list: Optional[List[str]] = []
    stock_list: List[str]
    period: Optional[str] = '1d'
    start_time: Optional[str] = ''
    end_time: Optional[str] = ''
    count: Optional[int] = -1
    dividend_type: Optional[str] = 'none'
    fill_data: Optional[bool] = True
    use_client_data: Optional[bool] = False

class GetTradingDatesRequest(BaseModel):
    market: str
    start_time: Optional[str] = ''
    end_time: Optional[str] = ''
    count: Optional[int] = -1

class DownloadFinancialDataRequest(BaseModel):
    stock_list: List[str]
    table_list: Optional[List[str]] = []

class DownloadFinancialData2Request(BaseModel):
    stock_list: List[str]
    table_list: Optional[List[str]] = []
    start_time: Optional[str] = ''
    end_time: Optional[str] = ''
    callback: Optional[str] = None

class GetFinancialDataRequest(BaseModel):
    stock_list: List[str]
    table_list: Optional[List[str]] = []
    start_time: Optional[str] = ''
    end_time: Optional[str] = ''
    report_type: Optional[str] = 'report_time'

class GetStockListInSectorRequest(BaseModel):
    sector_name: str

class GetInstrumentDetailRequest(BaseModel):
    stock_code: str

class GetMainContractRequest(BaseModel):
    stock_code: str

class GetDividFactorsRequest(BaseModel):
    stock_code: str
    start_time: Optional[str] = ''
    end_time: Optional[str] = ''

# --- API 实现 ---
@app.post("/download_history_data")
def download_history_data(req: DownloadHistoryDataRequest):
    logger.info(f"/download_history_data: {req.dict()}")
    try:
        if xtdata is None:
            return {"error": "xtquant.xtdata not available (调试模式)"}
        xtdata.download_history_data(
            req.stock_code,
            req.period,
            req.start_time,
            req.end_time,
            req.incrementally
        )
        return {"result": "ok"}
    except Exception as e:
        logger.error(f"download_history_data error: {e}\n{traceback.format_exc()}")
        return JSONResponse(status_code=500, content={"error": str(e)})

@app.post("/download_history_data2")
def download_history_data2(req: DownloadHistoryData2Request):
    logger.info(f"/download_history_data2: {req.dict()}")
    try:
        if xtdata is None:
            return {"error": "xtquant.xtdata not available (调试模式)"}
        # 回调进度日志
        def on_progress(data):
            logger.info(f"progress: {data}")
        xtdata.download_history_data2(
            req.stock_list,
            req.period,
            req.start_time,
            req.end_time,
            on_progress,
            req.incrementally
        )
        return {"result": "ok"}
    except Exception as e:
        logger.error(f"download_history_data2 error: {e}\n{traceback.format_exc()}")
        return JSONResponse(status_code=500, content={"error": str(e)})

@app.post("/get_market_data")
def get_market_data(req: GetMarketDataRequest):
    logger.info(f"/get_market_data: {req.dict()}")
    try:
        if xtdata is None:
            return {"error": "xtquant.xtdata not available (调试模式)"}
        data = xtdata.get_market_data(
            req.field_list,
            req.stock_list,
            req.period,
            req.start_time,
            req.end_time,
            req.count,
            req.dividend_type,
            req.fill_data
        )
        # DataFrame 转 dict
        result = {}
        for k, v in data.items():
            try:
                result[k] = v.to_dict() if hasattr(v, 'to_dict') else v
            except Exception as e:
                result[k] = str(v)
        return result
    except Exception as e:
        logger.error(f"get_market_data error: {e}\n{traceback.format_exc()}")
        return JSONResponse(status_code=500, content={"error": str(e)})

@app.post("/get_market_data_ex")
def get_market_data_ex(req: GetMarketDataRequest):
    logger.info(f"/get_market_data_ex: {req.dict()}")
    try:
        if xtdata is None:
            return {"error": "xtquant.xtdata not available (调试模式)"}
        data = xtdata.get_market_data_ex(
            req.field_list, 
            req.stock_list, 
            req.period, 
            req.start_time, 
            req.end_time, 
            req.count, 
            req.dividend_type, 
            req.fill_data)
        return {"result": data}
    except Exception as e:
        logger.error(f"get_market_data_ex error: {e}\n{traceback.format_exc()}")
        return JSONResponse(status_code=500, content={"error": str(e)})

@app.post("/get_local_data")
def get_local_data(req: GetLocalDataRequest):
    logger.info(f"/get_local_data: {req.dict()}")
    try:
        if xtdata is None:
            return {"error": "xtquant.xtdata not available (调试模式)"}
        kwargs = req.dict()
        use_client_data = kwargs.pop('use_client_data', False)
        # kwargs['data_dir'] = client_data_dir if use_client_data else None
        # remove  'use_client_data' from kwargs
        # kwargs.pop('use_client_data', None)
        print(kwargs)
        # data = xtdata.get_local_data(**kwargs)
        if use_client_data:
            data = xtdata.get_local_data(**kwargs, data_dir=client_data_dir)
        else:
            data = xtdata.get_local_data(**kwargs)
            
        result = {}
        for k, v in data.items():
            try:
                result[k] = v.to_dict() if hasattr(v, 'to_dict') else v
            except Exception as e:
                result[k] = str(v)
        return result
    except Exception as e:
        logger.error(f"get_local_data error: {e}\n{traceback.format_exc()}")
        return JSONResponse(status_code=500, content={"error": str(e)})

# --- 预留空壳接口 ---
@app.post("/other_api")
def other_api():
    return {"result": "not implemented"}

# --- 新增板块相关API ---
@app.post("/download_sector_data")
def download_sector_data():
    logger.info("/download_sector_data called")
    try:
        if xtdata is None:
            return {"error": "xtquant.xtdata not available (调试模式)"}
        xtdata.download_sector_data()
        return {"result": "ok"}
    except Exception as e:
        logger.error(f"download_sector_data error: {e}\n{traceback.format_exc()}")
        return JSONResponse(status_code=500, content={"error": str(e)})

@app.post("/get_sector_list")
def get_sector_list():
    logger.info("/get_sector_list called")
    try:
        if xtdata is None:
            return {"error": "xtquant.xtdata not available (调试模式)"}
        data = xtdata.get_sector_list()
        return {"result": data}
    except Exception as e:
        logger.error(f"get_sector_list error: {e}\n{traceback.format_exc()}")
        return JSONResponse(status_code=500, content={"error": str(e)})

@app.post("/download_holiday_data")
def download_holiday_data():
    logger.info("/download_holiday_data called")
    try:
        if xtdata is None:
            return {"error": "xtquant.xtdata not available (调试模式)"}
        xtdata.download_holiday_data()
        return {"result": "ok"}
    except Exception as e:
        logger.error(f"download_holiday_data error: {e}\n{traceback.format_exc()}")
        return JSONResponse(status_code=500, content={"error": str(e)})

@app.post("/get_holidays")
def get_holidays():
    logger.info("/get_holidays called")
    try:
        if xtdata is None:
            return {"error": "xtquant.xtdata not available (调试模式)"}
        data = xtdata.get_holidays()
        return {"result": data}
    except Exception as e:
        logger.error(f"get_holidays error: {e}\n{traceback.format_exc()}")
        return JSONResponse(status_code=500, content={"error": str(e)})

@app.post("/get_trading_dates")
def get_trading_dates(req: GetTradingDatesRequest):
    logger.info(f"/get_trading_dates: {req.dict()}")
    try:
        if xtdata is None:
            return {"error": "xtquant.xtdata not available (调试模式)"}
        data = xtdata.get_trading_dates(
            req.market,
            req.start_time,
            req.end_time,
            req.count
        )
        return {"result": data}
    except Exception as e:
        logger.error(f"get_trading_dates error: {e}\n{traceback.format_exc()}")
        return JSONResponse(status_code=500, content={"error": str(e)})

@app.post("/download_financial_data")
def download_financial_data(req: DownloadFinancialDataRequest):
    logger.info(f"/download_financial_data: {req.dict()}")
    try:
        if xtdata is None:
            return {"error": "xtquant.xtdata not available (调试模式)"}
        data = xtdata.download_financial_data(req.stock_list, req.table_list)
        return {"result": data}
    except Exception as e:
        logger.error(f"download_financial_data error: {e}\n{traceback.format_exc()}")
        return JSONResponse(status_code=500, content={"error": str(e)})

@app.post("/download_financial_data2")
def download_financial_data2(req: DownloadFinancialData2Request):
    logger.info(f"/download_financial_data2: {req.dict()}")
    try:
        if xtdata is None:
            return {"error": "xtquant.xtdata not available (调试模式)"}
        data = xtdata.download_financial_data2(req.stock_list, req.table_list, req.start_time, req.end_time, req.callback)
        return {"result": data}
    except Exception as e:
        logger.error(f"download_financial_data2 error: {e}\n{traceback.format_exc()}")
        return JSONResponse(status_code=500, content={"error": str(e)})

@app.post("/get_financial_data")
def get_financial_data(req: GetFinancialDataRequest):
    logger.info(f"/get_financial_data: {req.dict()}")
    try:
        if xtdata is None:
            return {"error": "xtquant.xtdata not available (调试模式)"}
        data = xtdata.get_financial_data(
            req.stock_list,
            req.table_list,
            req.start_time,
            req.end_time,
            req.report_type
        )
        # dict of DataFrame, convert to dict
        result = {}
        for stock, tables in data.items():
            result[stock] = {}
            for table, df in tables.items():
                try:
                    # 将 DataFrame中的NaN/inf 替换为 None
                    df = df.replace([np.inf, np.nan], None)
                    # result[stock][table] = df.to_dict() if hasattr(df, 'to_dict') else df
                    result[stock][table] = df
                except Exception as e:
                    result[stock][table] = str(df)
        return result
    except Exception as e:
        logger.error(f"get_financial_data error: {e}\n{traceback.format_exc()}")
        return JSONResponse(status_code=500, content={"error": str(e)})

@app.post("/get_stock_list_in_sector")
def get_stock_list_in_sector(req: GetStockListInSectorRequest):
    logger.info(f"/get_stock_list_in_sector: {req.dict()}")
    try:
        if xtdata is None:
            return {"error": "xtquant.xtdata not available (调试模式)"}
        data = xtdata.get_stock_list_in_sector(req.sector_name)
        return {"result": data}
    except Exception as e:
        logger.error(f"get_stock_list_in_sector error: {e}\n{traceback.format_exc()}")
        return JSONResponse(status_code=500, content={"error": str(e)})

@app.post("/get_instrument_detail")
def get_instrument_detail(req: GetInstrumentDetailRequest):
    logger.info(f"/get_instrument_detail: {req.dict()}")
    try:
        if xtdata is None:
            return {"error": "xtquant.xtdata not available (调试模式)"}
        data = xtdata.get_instrument_detail(req.stock_code)
        return {"result": data}
    except Exception as e:
        logger.error(f"get_instrument_detail error: {e}\n{traceback.format_exc()}")
        return JSONResponse(status_code=500, content={"error": str(e)})

@app.post("/get_main_contract")
def get_main_contract(req: GetMainContractRequest):
    logger.info(f"/get_main_contract: {req.dict()}")
    try:
        if xtdata is None:
            return {"error": "xtquant.xtdata not available (调试模式)"}
        data = xtdata.get_main_contract(req.stock_code)
        return {"result": data}
    except Exception as e:
        logger.error(f"get_main_contract error: {e}\n{traceback.format_exc()}")
        return JSONResponse(status_code=500, content={"error": str(e)})

@app.post("/get_divid_factors")
def get_divid_factors(req: GetDividFactorsRequest):
    logger.info(f"/get_divid_factors: {req.dict()}")
    try:
        if xtdata is None:
            return {"error": "xtquant.xtdata not available (调试模式)"}
        data = xtdata.get_divid_factors(req.stock_code)
        return {"result": data}
    except Exception as e:
        logger.error(f"get_divid_factors error: {e}\n{traceback.format_exc()}")
        return JSONResponse(status_code=500, content={"error": str(e)})
    
@app.get("/", tags=["system"])
def test_root():
    return {"status": "ok", "message": "The micro service for xtquant helper is running"}

# --- 启动入口 ---
if __name__ == "__main__":
    sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    uvicorn.run("core.api_service:app", host="0.0.0.0", port=8100, reload=True)