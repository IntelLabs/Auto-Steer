-- TPC-H Query 11

select
        ps.partkey,
        sum(ps.supplycost * ps.availqty) as "value"
from
        partsupp ps,
        supplier s,
        nation n
where
        ps.suppkey = s.suppkey
        and s.nationkey = n.nationkey
        and n.name = 'GERMANY'
group by
        ps.partkey having
                sum(ps.supplycost * ps.availqty) > (
                        select
                                sum(ps2.supplycost * ps2.availqty) * 0.0001
                        from
                                partsupp ps2,
                                supplier s2,
                                nation n2
                        where
                                ps2.suppkey = s2.suppkey
                                and s2.nationkey = n2.nationkey
                                and n2.name = 'GERMANY'
                )
order by
        "value" desc
