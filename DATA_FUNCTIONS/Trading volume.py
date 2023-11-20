from flipside import Flipside
import pandas as pd
from datetime import datetime, timedelta

start_date = datetime(2020, 8, 1, 0, 0, 0)
end_date = datetime(2023, 8, 10, 0, 0, 0)
flipside = Flipside(
    "api-key", "https://api-v2.flipsidecrypto.xyz"
)
result_df = pd.DataFrame(columns=["Date", "Trading Volume"])
current_date = start_date
while current_date <= end_date:
    print(current_date)
    formatted_date = current_date.strftime("%Y-%m-%d %H:%M:%S")
    sql = f"""SELECT SUM(AMOUNT_OUT_USD) AS TRADING_VOLUME FROM ethereum.defi.ez_dex_swaps 
                WHERE platform = 'curve' AND CAST(BLOCK_TIMESTAMP AS DATE) = '{formatted_date}';"""
    query_result_set = flipside.query(sql)
    if query_result_set.records:
        trading_volume = query_result_set.records[0]["trading_volume"]
    else:
        trading_volume = None

    result_df = pd.concat(
        [
            result_df,
            pd.DataFrame(
                {"Date": formatted_date, "Trading Volume": trading_volume}, index=[0]
            ),
        ],
        ignore_index=True,
    )

    current_date += timedelta(days=1)

current_page_number = 1
page_size = 100
total_pages = 2
all_rows = []

while current_page_number <= total_pages:
    results = flipside.get_query_results(
        query_result_set.query_id, page_number=current_page_number, page_size=page_size
    )

    total_pages = results.page.totalPages

    if results.records:
        trading_volumes = [record["trading_volume"] for record in results.records]
        all_rows = all_rows + trading_volumes
    current_page_number += 1
result_df.to_parquet("CURVE_TV_2023-08-01_to_2023-08-10.parquet", index=False)
