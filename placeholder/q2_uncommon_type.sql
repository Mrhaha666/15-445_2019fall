with longest(type, runtime_minutes) as (
    select type, max(runtime_minutes) 
    from titles
    group by type
)
select titles.type, primary_title, titles.runtime_minutes
from titles, longest
where titles.runtime_minutes = longest.runtime_minutes and titles.type = longest.type
order by titles.type, primary_title desc;

