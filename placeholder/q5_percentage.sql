with sum_premiered(total) as (
    select count(*)
    from titles
)

select  (premiered / 10 * 10) || 's', ROUND((count(*) * 1.0 / total * 100), 4) as per
from titles join sum_premiered
where premiered is not null
group by (premiered / 10 * 10)
order by per desc;