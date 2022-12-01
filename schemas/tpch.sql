-- Sccsid:     @(#)dss.ddl	2.1.8.1

CREATE TABLE nation
(
    nationkey  INTEGER not null,
    name       CHAR(25) not null,
    regionkey  INTEGER not null,
    comment    VARCHAR(152)
);

CREATE TABLE region
(
    regionkey  INTEGER not null,
    name       CHAR(25) not null,
    comment    VARCHAR(152)
);

CREATE TABLE part
(
    partkey     BIGINT not null,
    name        VARCHAR(55) not null,
    mfgr        CHAR(25) not null,
    brand       CHAR(10) not null,
    type        VARCHAR(25) not null,
    size        INTEGER not null,
    container   CHAR(10) not null,
    retailprice DOUBLE PRECISION not null,
    comment     VARCHAR(23) not null
);

CREATE TABLE supplier
(
    suppkey     BIGINT not null,
    name        CHAR(25) not null,
    address     VARCHAR(40) not null,
    nationkey   INTEGER not null,
    phone       CHAR(15) not null,
    acctbal     DOUBLE PRECISION not null,
    comment     VARCHAR(101) not null
);

CREATE TABLE partsupp
(
    partkey     BIGINT not null,
    suppkey     BIGINT not null,
    availqty    BIGINT not null,
    supplycost  DOUBLE PRECISION  not null,
    comment     VARCHAR(199) not null
);

CREATE TABLE customer
(
    custkey     BIGINT not null,
    name        VARCHAR(25) not null,
    address     VARCHAR(40) not null,
    nationkey   INTEGER not null,
    phone       CHAR(15) not null,
    acctbal     DOUBLE PRECISION   not null,
    mktsegment  CHAR(10) not null,
    comment     VARCHAR(117) not null
);

CREATE TABLE orders
(
    orderkey       BIGINT not null,
    custkey        BIGINT not null,
    orderstatus    CHAR(1) not null,
    totalprice     DOUBLE PRECISION not null,
    orderdate      DATE not null,
    orderpriority  CHAR(15) not null,  
    clerk          CHAR(15) not null, 
    shippriority   INTEGER not null,
    comment        VARCHAR(79) not null
);

CREATE TABLE lineitem
(
    orderkey    BIGINT not null,
    partkey     BIGINT not null,
    suppkey     BIGINT not null,
    linenumber  BIGINT not null,
    quantity    DOUBLE PRECISION not null,
    extendedprice  DOUBLE PRECISION not null,
    discount    DOUBLE PRECISION not null,
    tax         DOUBLE PRECISION not null,
    returnflag  CHAR(1) not null,
    linestatus  CHAR(1) not null,
    shipdate    DATE not null,
    commitdate  DATE not null,
    receiptdate DATE not null,
    shipinstruct CHAR(25) not null,
    shipmode     CHAR(10) not null,
    comment      VARCHAR(44) not null
);
