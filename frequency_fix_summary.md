# Minute Frequency Parameter Fix - Complete Solution

## Problem
When updating minute_5 (or other minute frequencies), the system was always sending "1m" to xtquant_helper instead of the correct frequency (e.g., "5m"). This caused all minute data requests to return 1-minute data regardless of the requested frequency.

## Root Causes
Multiple issues in the data flow chain prevented the frequency parameter from reaching the provider:

### Issue 1: Provider Level
In `finance_data_hub/providers/xtquant.py`, the `get_incremental_data()` method was not extracting the frequency from the `data_type` parameter.

### Issue 2: Router Level (Critical!)
In `finance_data_hub/router/smart_router.py`, the `route()` method received the `freq` parameter for routing decisions but **didn't pass it to the provider method** in `**kwargs`. This was the main bottleneck!

### Issue 3: CLI Level
In `finance_data_hub/cli/main.py`, the frequency mapping was incomplete, only covering minute_1 and minute_5, missing minute_15, minute_30, and minute_60.

## Solutions

### Solution 1: Provider Level - Extract Frequency from data_type

**File:** `finance_data_hub/providers/xtquant.py`

**Before (Line 707):**
```python
elif data_type.startswith("minute"):
    freq = kwargs.get("freq", "1m")  # Always defaulted to "1m"
    return self._get_incremental_minute(symbol, start_date, end_date, freq)
```

**After (Lines 733-742):**
```python
elif data_type.startswith("minute"):
    # 从data_type中提取频率 (e.g., "minute_5" -> "5m")
    if "_" in data_type:
        minute_freq = data_type.split("_")[1]  # "minute_5" -> "5"
        freq = kwargs.get("freq", f"{minute_freq}m")  # Default to "5m"
        logger.debug(f"Extracted frequency from data_type '{data_type}': {freq}")
    else:
        freq = kwargs.get("freq", "1m")  # Default for "minute"
        logger.debug(f"Using default frequency for data_type '{data_type}': {freq}")
    return self._get_incremental_minute(symbol, start_date, end_date, freq)
```

### Solution 2: Router Level - Pass freq to Provider Method ⭐

**File:** `finance_data_hub/router/smart_router.py`

**Before (Line 419):**
```python
result = method(**kwargs)
```

**After (Lines 419-424):**
```python
# 如果有 freq 参数，将其添加到 kwargs 中传递给 provider 方法
if freq:
    kwargs["freq"] = freq
    logger.debug(f"Passing freq={freq} to provider method")

result = method(**kwargs)
```

⭐ **This is the critical fix** - without this, the freq parameter was never reaching the provider methods!

### Solution 3: CLI Level - Complete Frequency Mapping

**File:** `finance_data_hub/cli/main.py`

**Before (Lines 316-317 and 443-444):**
```python
freq_map = {"minute_1": "1m", "minute_5": "5m"}
actual_freq = freq_map.get(data_type, "1m")
```

**After (Lines 317-328 in smart download, 444-455 in force update):**
```python
# 从 data_type 中提取频率
freq_map = {
    "minute_1": "1m",
    "minute_5": "5m",
    "minute_15": "15m",
    "minute_30": "30m",
    "minute_60": "60m",
    "minute": "1m",  # 默认
}
actual_freq = freq_map.get(data_type, "1m")

if verbose:
    console.print(f"[dim]  频率映射: {data_type} -> {actual_freq}[/dim]")
```

## Data Flow After Fix

When running:
```bash
fdh-cli update --symbol 000001.SZ --dataset minute_5 --start-date 2025-11-21 --end-date 2025-11-21
```

The data flows through the system as follows:

1. **CLI** (`main.py:317-328`):
   - Receives `dataset="minute_5"`
   - Maps to `freq="5m"` using freq_map
   - Calls `updater.update_minute_data(symbols=[...], freq="5m")`

2. **DataUpdater** (`updater.py:264-272`):
   - Receives `freq="5m"`
   - Calls `router.route(asset_class="stock", data_type="minute", freq="5m", method_name="get_minute_data", symbol=..., start_date=..., end_date=...)`

3. **SmartRouter** (`smart_router.py:419-424`): ⭐
   - Uses `freq="5m"` for routing decisions (select appropriate provider)
   - **NEW:** Adds `freq="5m"` to kwargs before calling provider method
   - Calls `provider.get_minute_data(symbol=..., start_date=..., end_date=..., freq="5m")`

4. **XTQuantProvider** (`xtquant.py:462-471, 733-742`):
   - Receives `freq="5m"` parameter
   - Maps `"5m"` to xtquant format using freq_mapping (5m stays as "5m", 60m becomes "1h")
   - Sends API request with `period="5m"` to xtquant_helper

## Enhanced Logging

Added detailed logging throughout the data flow:

**CLI Level** (`main.py:328, 455`):
```python
console.print(f"[dim]  频率映射: {data_type} -> {actual_freq}[/dim]")
```

**Router Level** (`smart_router.py:422`):
```python
logger.debug(f"Passing freq={freq} to provider method")
```

**Provider Level** (`xtquant.py:711, 714, 471, 486, 501`):
```python
logger.debug(f"Extracted frequency from data_type '{data_type}': {freq}")
logger.info(f"Frequency mapping: {freq} -> {xtquant_freq}")
logger.debug(f"Download payload: period={xtquant_freq}, start={start_date[:8]}, end={end_date[:8]}")
logger.debug(f"Getting local data with payload: period={xtquant_freq}, start={start_date[:8]}, end={end_date[:8]}")
```

## Testing
Created `test_frequency_extraction.py` to verify the frequency extraction logic:
- ✅ minute_1 → 1m
- ✅ minute_5 → 5m
- ✅ minute_15 → 15m
- ✅ minute_30 → 30m
- ✅ minute_60 → 60m
- ✅ minute → 1m (fallback)

## Expected Behavior After Fix

When running minute_5 data update with `--verbose`, the logs should show:

```
资产类别: stock
数据类型: minute_5
...
  频率映射: minute_5 -> 5m
DEBUG: Passing freq=5m to provider method
INFO: Fetching 5m data for 000001.SZ from 2025-11-21 to 2025-11-21
INFO: Frequency mapping: 5m -> 5m
DEBUG: Download payload: period=5m, start=20251121, end=20251121
DEBUG: Getting local data with payload: period=5m, start=20251121, end=20251121
```

And xtquant_helper should receive API requests with `period: "5m"` instead of `period: "1m"`.

## Files Modified

1. **finance_data_hub/providers/xtquant.py**
   - Lines 733-742: Extract frequency from data_type in get_incremental_data()
   - Lines 471, 486, 501: Add logging for frequency mapping and API payloads

2. **finance_data_hub/router/smart_router.py** ⭐
   - Lines 419-424: Pass freq parameter to provider methods via kwargs (critical fix!)

3. **finance_data_hub/cli/main.py**
   - Lines 317-328: Complete frequency mapping in smart download mode
   - Lines 444-455: Complete frequency mapping in force update mode
   - Add verbose logging for frequency mapping

4. **test_frequency_extraction.py** (new)
   - Unit tests for frequency extraction logic

## Verification Checklist

To verify the fix is working:

- [ ] Run update with `--verbose` flag to see frequency mapping logs
- [ ] Check router logs show "Passing freq=5m to provider method"
- [ ] Check provider logs show correct frequency mapping
- [ ] Verify xtquant_helper receives requests with correct period parameter
- [ ] Verify database stores 5-minute data (not 1-minute data)
- [ ] Test with other frequencies (15m, 30m, 60m) to ensure they work too

## Next Steps

1. Test with actual minute_5 data update
2. Verify xtquant_helper logs to confirm it receives period=5m
3. Check database to ensure 5-minute interval data is stored
4. Consider whether database schema needs modification to track frequency per record
