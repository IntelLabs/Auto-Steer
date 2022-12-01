-- TPC-H Query 15

with revenue as (
	select
		l.suppkey as supplier_no,
		sum(l.extendedprice * (1 - l.discount)) as total_revenue
	from
		lineitem l
	where
		l.shipdate >= date '1996-01-01'
		and l.shipdate < date '1996-04-01'
	group by
		l.suppkey)
select
	s.suppkey,
	s.name,
	s.address,
	s.phone,
	r.total_revenue
from
	supplier s,
	revenue r
where
	s.suppkey = supplier_no
	and r.total_revenue = (
		select
			max(r2.total_revenue)
		from
			revenue r2
	)
order by
	s.suppkey
