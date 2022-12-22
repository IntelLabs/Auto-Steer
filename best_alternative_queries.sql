

WITH default_plans (query_path, walltime) AS
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
            qoc.disabled_rules), -- default for queries that timed out
     results(query_path, num_disabled_rules, runtime, runtime_baseline, savings, disabled_rules, rank) AS
  (SELECT q.query_path,
          qoc.num_disabled_rules,
          median(m.walltime),
          dp.walltime,
          (dp.walltime * 1.0 - median(m.walltime)) / dp.walltime  AS savings,
          qoc.disabled_rules,
          dense_rank() OVER (PARTITION BY q.query_path
                             ORDER BY (dp.walltime - median(m.walltime)) / dp.walltime DESC) AS ranki
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
            dp.walltime
   ORDER BY savings DESC)
SELECT *
FROM results
WHERE rank = 1
  AND query_path like '%%:path%%'
ORDER BY savings DESC;