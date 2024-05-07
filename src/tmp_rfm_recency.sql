DELETE FROM analysis.tmp_rfm_recency;

WITH t AS (
    SELECT u.id as user_id,
	        max(CASE WHEN oss."key" = 'Closed' THEN order_ts END) max_dttm,
	        sum(CASE WHEN oss."key" = 'Closed' THEN o.payment END) order_payment,
	        sum(CASE WHEN oss."key" = 'Closed' THEN 1 ELSE 0 END) order_count
    FROM analysis.users u
    LEFT JOIN analysis.orders o ON u.id = o.user_id
    LEFT JOIN analysis.orderstatuses oss ON oss.id = o.status
    GROUP BY u.id
), t2 AS (
    SELECT user_id,
            max_dttm,
            ntile(5) OVER (ORDER BY coalesce(max_dttm, '01/01/1990'::TIMESTAMP)) tile
	FROM t
) INSERT analysis.tmp_rfm_recency (
    SELECT user_id, tile
    FROM t2
);