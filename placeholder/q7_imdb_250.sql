with 
    average(average_rating) as(
        select sum(rating * votes) / sum(votes)
        from ratings, titles
        where titles.title_id = ratings.title_id and titles.type = 'movie'
    ),
    mn(min_votes) as (select 25000.0)


select primary_title, (votes / (votes + min_votes)) * rating + (min_votes / (min_votes + votes)) * average_rating as wr
from ratings, titles, mn, average
where titles.title_id = ratings.title_id and titles.type = 'movie'
order by wr desc
limit 250;
