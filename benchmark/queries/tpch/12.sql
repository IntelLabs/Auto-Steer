-- TPC-H Query 12

select
        l.shipmode,
        sum(case
                when o.orderpriority = '1-URGENT'
                        or o.orderpriority = '2-HIGH'
                        then 1
                else 0
        end) as high_line_count,
        sum(case
                when o.orderpriority <> '1-URGENT'
                        and o.orderpriority <> '2-HIGH'
                        then 1
                else 0
        end) as low_line_count
from
        orders o,
        lineitem l
where
        o.orderkey = l.orderkey
        and l.shipmode in ('MAIL', 'SHIP')
        and l.commitdate < l.receiptdate
        and l.shipdate < l.commitdate
        and l.receiptdate >= date '1994-01-01'
        and l.receiptdate < date '1995-01-01'
group by
        l.shipmode
order by
        l.shipmode
