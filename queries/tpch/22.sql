-- TPC-H Query 22

select
        cntrycode,
        count(*) as numcust,
        sum(acctbal) as totacctbal
from
        (
                select
                        substring(c.phone from 1 for 2) as cntrycode,
                        c.acctbal
                from
                        customer c
                where
                        substring(c.phone from 1 for 2) in
                                ('13', '31', '23', '29', '30', '18', '17')
                        and c.acctbal > (
                                select
                                        avg(c2.acctbal)
                                from
                                        customer c2
                                where
                                        c2.acctbal > 0.00
                                        and substring(c2.phone from 1 for 2) in
                                                ('13', '31', '23', '29', '30', '18', '17')
                        )
                        and not exists (
                                select
                                        *
                                from
                                        orders o
                                where
                                        o.custkey = c.custkey
                        )
        ) as custsale
group by
        cntrycode
order by
        cntrycode
