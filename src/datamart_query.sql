DELETE FROM analysis.dm_rfm_segments;
INSERT INTO analysis.dm_rfm_segments(user_id, recency, frequency, monetary_value)
(
    SELECT user_id,
           max(recency),
           max(frequency),
           max(monetary_value)
    FROM (SELECT t.user_id,
                 t.recency,
                 0::int2 AS frequency,
                 0::int2 AS monetary_value
          FROM analysis.tmp_rfm_recency  t
          UNION ALL
          SELECT trf.user_id, 0, trf.frequency, 0
          FROM analysis.tmp_rfm_frequency trf
          UNION ALL
          SELECT trmv.user_id, 0, 0, trmv.monetary_value
          FROM analysis.tmp_rfm_monetary_value trmv
) t
    GROUP BY user_id);