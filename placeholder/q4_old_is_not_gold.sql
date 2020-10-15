select  (premiered / 10 * 10) || 's', count(*) as pt
from titles
where premiered is not null
group by (premiered / 10 * 10)
order by pt desc