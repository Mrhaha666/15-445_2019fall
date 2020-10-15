select type, count(*) as cnt
from titles
group by type
order by cnt ASC;