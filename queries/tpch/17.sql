-- TPC-H Query 17

select
        sum(l.extendedprice) / 7.0 as avg_yearly
from
        lineitem l,
        part p
where
        p.partkey = l.partkey
        and p.brand = 'Brand#23'
        and p.container = 'MED BOX'
        and l.quantity < (
                select
                        0.2 * avg(l2.quantity)
                from
                        lineitem l2
                where
                        l2.partkey = p.partkey
        )
