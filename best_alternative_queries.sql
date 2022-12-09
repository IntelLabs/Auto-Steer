WITH default_plans (query_path, running_time) AS
  (SELECT q.query_path,
          median(walltime)
   FROM queries q,
        query_optimizer_configs qoc,
        measurements m
   WHERE q.id = qoc.query_id
     AND qoc.id = m.query_optimizer_config_id
     AND qoc.num_disabled_rules = 0
     AND qoc.disabled_rules = 'None'
   GROUP BY q.query_path,
            qoc.num_disabled_rules,
            qoc.disabled_rules
   HAVING median(elapsed) < 1000000000),
     results(query_path, num_disabled_rules, runtime, runtime_baseline, savings, disabled_rules, rank) AS
  (SELECT q.query_path,
          qoc.num_disabled_rules,
          median(m.elapsed),
          dp.running_time,
          (dp.running_time - median(m.elapsed)) / dp.running_time AS savings,
          qoc.disabled_rules,
          dense_rank() OVER (PARTITION BY q.query_path
                             ORDER BY (dp.running_time - median(m.elapsed)) / dp.running_time DESC) AS ranki
   FROM queries q,
        query_optimizer_configs qoc,
        measurements m,
        default_plans dp
   WHERE q.id = qoc.query_id
     AND qoc.id = m.query_optimizer_config_id
     AND dp.query_path = q.query_path
     AND qoc.num_disabled_rules > 0
   GROUP BY q.query_path,
            qoc.num_disabled_rules,
            qoc.disabled_rules,
            dp.running_time
   ORDER BY savings DESC)
SELECT *
FROM results
WHERE rank = 1
  AND query_path like '%%:path%%'
ORDER BY savings DESC;