DELETE FROM analysis.tmp_rfm_monetary_value ;

WITH t AS
(
    SELECT u.id AS user_id,
	        max(case when oss."key" = 'Closed' then order_ts end) max_dttm,
	        sum(case when oss."key" = 'Closed' then o.payment end) order_payment,
	        sum(case when oss."key" = 'Closed' then 1 else 0 end) order_count,
    FROM analysis.users u
    LEFT JOIN analysis.orders o on u.id = o.user_id
    LEFT JOIN analysis.orderstatuses oss on oss.id = o.status
    GROUP BY u.id
), t2 AS (
    SEECT user_id,
            order_payment,
            ntile(5) over (order by order_payment asc) tile
	FROM t
) INSERT INTO analysis.tmp_rfm_monetary_value
(
    SEECT user_id, tile
    FROM t2
);