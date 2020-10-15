with MH_titles(title_id) as(
    select title_id
    from crew 
    where 
        person_id in
        (
            select person_id
            from people
            where name ='Mark Hamill' and born = 1951
        )
)


select count(distinct person_id)
from crew
where 
    title_id in MH_titles
    and 
    (category = "actor" OR category = "actress");