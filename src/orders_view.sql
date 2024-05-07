DROP VIEW IF EXISTS analysis.orders ;

CREATE VIEW analysis.orders AS
WITH t AS (SELECT osl.order_id,
                  osl.status_id,
  	              last_value(osl.status_id) OVER (PARTITION BY osl.order_id ORDER BY osl.dttm ASC
				 RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) log_last_status_id
           FROM production.orderstatuslog osl
), t2 AS (SELECT t.order_id, min(t.log_last_status_id) log_last_status_id
		  FROM t
		  WHERE t.status_id = t.log_last_status_id
		  GROUP BY t.order_id)
SELECT o.order_id,
       o.order_ts,
       t2.log_last_status_id AS status,
       o.user_id,
       o.bonus_payment,
       o.payment,
       o."cost",
       o.bonus_grant
FROM production.orders o
LEFT JOIN t2 ON o.order_id = t2.order_id