# Auto-Steer

> [!CAUTION]
> **PROJECT NOT UNDER ACTIVE MANAGEMENT**
> * This project will no longer be maintained by Intel.
> * Intel has ceased development and contributions including, but not limited to, maintenance, bug fixes, new releases, or updates, to this project.  
> * Intel no longer accepts patches to this project.  
> * If you have an ongoing need to use this project, are interested in independently developing it, or would like to maintain patches for the open source software community, please create your own fork of this project.  

## License

This prototype implementation is licensed under the 'MIT license' (see LICENSE).

## Requirements

### Packages

- sqlite3
    - Statistics extension (we provide a download script: `sqlean-extensions/download.sh`)
- python3 (at least version 3.10)

### Python3 requirements

- Install python requirements using the file `pip3 install -r requirements.txt`

## Run Auto-Steer

### Database Systems

#### Auto-Steer-G already supports five open-source database systems:

- PostgreSQL
    - We tested AutoSteer with PostgreSQL 13
- PrestoDB
    - PrestoDB does not expose many rewrite rules. Therefore, the following patch exposes the top-7 hints we found in
      our experiments.
    - Get the most recent version of [PrestoDB](https://github.com/prestodb/presto)
    - Apply the PrestoDB patch : `git apply Presto-disable-optimizers-through-session-properties.patch`
    - Build PrestoDB from source and start the server
- MySQL
    - We tested AutoSteer with MySQL 8
- DuckDB
    - Install the DuckDB-python package via `pip`
- SparkSQL
    - We run SparkSQL using the official Docker image of its most recent version

#### DBMS Configuration

Depending on your custom installation and DBMS setup, add the required information to the `configs/<dbms>.cfg`-file.

### Executing Auto-Steer's Training Mode

Auto-Steer's training mode execution consists of two steps:

1. (A) Approximate the query span, and (B) run the dynamic programming-based hint-set exploration
   ```commandline
   main.py --training --database {postgres|presto|mysql|duckdb|spark} --benchmark {path-to-sql-queries}
   ```
2. By now, Auto-Steer persisted all generated training data (e.g. query plans and execution statistics) in a
   sqlite-database that can be found under `results/<database>.sqlite`.
3. For PrestoDB query plans, we implemented the preprocessing of query plans for tree convolutional neural networks.
   ```commandline
   main.py --inference --database presto --benchmark {path-to-sql-queries}
   ```
4. The inference results can be found in the directory `evaluation`.

## Code Formatting

- All python files will be checked using `pylint` before they can be comitted. The code style is primarily based on
  the [Google Python Style Guide](https://google.github.io/styleguide/pyguide.html).
  However, it allows longer lines (160 characters).
- Please, install and run pylint (there is also a git pre-commit hook) before committing

## Cite
If you use AutoSteer in your work, please cite us:
```
@article{autosteer2023,
    author       = {Anneser, Christoph and Tatbul, Nesime and Cohen, David and Xu, Zhenggang and Pandian, Prithviraj and Laptev, Nikolay and Marcus, Ryan},
    date         = {2023},
    journaltitle = {PVLDB},
    number       = {12},
    pages        = {3515--3527},
    title        = {AutoSteer: Learned Query Optimization for Any SQL Database},
    volume       = {16},
}
```
