CREATE TABLE IF NOT EXISTS queries
(
    id                 INTEGER PRIMARY KEY,
    query_path         varchar(256) UNIQUE,
    result_fingerprint INTEGER DEFAULT 0
);
--------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS query_required_optimizers
(
    query_id     INTEGER REFERENCES queries,
    optimizer TEXT,
    PRIMARY KEY (query_id, optimizer)
);
--------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS query_effective_optimizers
(
    query_id     INTEGER REFERENCES queries,
    optimizer TEXT,
    PRIMARY KEY (query_id, optimizer)
);
--------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS query_effective_optimizers_dependencies
(
    query_id     INTEGER REFERENCES queries,
    optimizer TEXT,
    dependent_optimizer TEXT, -- dependency of 'optimizer'
    PRIMARY KEY (query_id, optimizer, dependent_optimizer)
);
--------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS query_optimizer_configs
(
    id                   INTEGER PRIMARY KEY,
    query_id             INTEGER REFERENCES queries,
    disabled_rules       TEXT, -- this column represents exactly one hint-set
    query_plan           TEXT,
    num_disabled_rules   INTEGER,
    hash                 INTEGER, -- the hash value of the optimizer query plan
    duplicated_plan      BOOLEAN DEFAULT FALSE,
    UNIQUE (query_id, disabled_rules)
);
--------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS measurements
(
    query_optimizer_config_id INTEGER REFERENCES query_optimizer_configs,
    walltime                  INTEGER,
    machine                   TEXT,
    time                      TIMESTAMP,
    input_data_size           INTEGER,
    nodes                     INTEGER
);
--------------------------------------------------------------------------------
