-- TPC-H Query 18

select
        c.name,
        c.custkey,
        o.orderkey,
        o.orderdate,
        o.totalprice,
        sum(l.quantity)
from
        customer c,
        orders o,
        lineitem l
where
        o.orderkey in (
                select
                        l2.orderkey
                from
                        lineitem l2
                group by
                        l2.orderkey having
                                sum(l2.quantity) > 300
        )
        and c.custkey = o.custkey
        and o.orderkey = l.orderkey
group by
        c.name,
        c.custkey,
        o.orderkey,
        o.orderdate,
        o.totalprice
order by
        o.totalprice desc,
        o.orderdate
limit 100
