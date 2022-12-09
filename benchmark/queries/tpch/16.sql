-- TPC-H Query 16

select
        p.brand,
        p.type,
        p.size,
        count(distinct ps.suppkey) as supplier_cnt
from
        partsupp ps,
        part p
where
        p.partkey = ps.partkey
        and p.brand <> 'Brand#45'
        and p.type not like 'MEDIUM POLISHED%'
        and p.size in (49, 14, 23, 45, 19, 3, 36, 9)
        and ps.suppkey not in (
                select
                        s.suppkey
                from
                        supplier s
                where
                        s.comment like '%Customer%Complaints%'
        )
group by
        p.brand,
        p.type,
        p.size
order by
        supplier_cnt desc,
        p.brand,
        p.type,
        p.size
