-- TPC-H Query 13

select
        c_count,
        count(*) as custdist
from
        (
                select
                        c.custkey,
                        count(o.orderkey) c_count
                from
                        customer c left outer join orders o on
                                c.custkey = o.custkey
                                and o.comment not like '%special%requests%'
                group by
                        c.custkey
        ) as c_orders
group by
        c_count
order by
        custdist desc,
        c_count desc
