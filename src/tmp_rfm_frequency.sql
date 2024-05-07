DELETE FROM analysis.tmp_rfm_frequency;

WITH t AS (
    SELECT u.id AS user_id,
	       max(CASE WHEN oss."key" = 'Closed' THEN order_ts END) max_dttm,
	       sum(CASE WHEN oss."key" = 'Closed' THEN o.payment END) order_payment,
	       sum(CASE WHEN oss."key" = 'Closed' THEN 1 ELSE 0 END) order_count
    FROM analysis.users u
    LEFT JOIN analysis.orders o ON u.id = o.user_id
    LEFT JOIN analysis.orderstatuses oss ON oss.id = o.status
    GROUP BY u.id
), t2 AS (
    SELECT user_id,
           order_count,
           ntile(5) OVER (ORDER BY order_count) tile
	FROM t)
INSERT INTO analysis.tmp_rfm_frequency
(
    SELECT user_id, tile
    FROM t2
);