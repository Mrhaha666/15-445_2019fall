with title_dobbedtimes(title_id,primary_title, dobbed_times) as(
    select titles.title_id, primary_title, count(*)
    from titles, akas
    where titles.title_id = akas.title_id
    group by titles.title_id
)

select title_id,primary_title, dobbed_times
from title_dobbedtimes
order by dobbed_times desc
limit 10;