from unittest.mock import AsyncMock, MagicMock, Mock

import pandas as pd
import pytest

from finance_data_hub.database.operations import DataOperations


@pytest.mark.asyncio
async def test_insert_sw_industry_classify_counts_asyncpg_executemany_records():
    manager = Mock()
    manager._engine = Mock()
    connection = AsyncMock()
    transaction = MagicMock()
    transaction.__aenter__ = AsyncMock(return_value=connection)
    transaction.__aexit__ = AsyncMock(return_value=False)
    manager._engine.begin.return_value = transaction
    connection.execute.return_value = Mock(rowcount=-1)
    operations = DataOperations(manager)

    count = await operations.insert_sw_industry_classify_batch(
        pd.DataFrame(
            {
                "index_code": ["801010.SI", "801020.SI"],
                "industry_name": ["农林牧渔", "煤炭"],
                "parent_code": ["0", "0"],
                "level": ["L1", "L1"],
                "industry_code": ["801010", "801020"],
                "is_pub": ["1", "1"],
                "src": ["SW2021", "SW2021"],
            }
        )
    )

    assert count == 2
