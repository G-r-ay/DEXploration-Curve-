WITH DailySwaps AS (
    SELECT
        DATE_TRUNC('day', BLOCK_TIMESTAMP) AS DATE,
        COUNT(*) AS swap_count
    FROM
        ethereum.defi.ez_dex_swaps
    WHERE
        PLATFORM = 'curve'
        AND POOL_NAME = 'DAI-USDC-USDT'
    GROUP BY
        DATE
)

SELECT
    ds.DATE,
    ds.swap_count,
    SUM(s.AMOUNT_OUT_USD) AS total_trading_volume_usd
FROM
    DailySwaps ds
JOIN
    ethereum.defi.ez_dex_swaps s
ON
    DATE_TRUNC('day', s.BLOCK_TIMESTAMP) = ds.DATE
WHERE
    PLATFORM = 'curve'
    AND CAST(s.BLOCK_TIMESTAMP AS DATE) >= '2022-2-11'
    AND CAST(s.BLOCK_TIMESTAMP AS DATE) < '2023-11-13'
    AND s.POOL_NAME = 'DAI-USDC-USDT'
GROUP BY
    ds.DATE, ds.swap_count
ORDER BY
    ds.DATE;
