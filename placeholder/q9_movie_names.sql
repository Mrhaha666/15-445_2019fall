with MH_GL_titles(title_id) as(
    select MH_title.title_id
    from 
        (
            select title_id
            from crew 
            where person_id in
            (
            select person_id
            from people
            where (name = 'Mark Hamill' and born = 1951) 
            )
        ) as MH_title,
        (
            select title_id
            from crew 
            where person_id in
            (
                select person_id
                from people
                where (name = 'George Lucas' and born = 1944) 
            )
        ) as GL_titles
    where GL_titles.title_id = MH_title.title_id
)



select titles.primary_title
from titles, MH_GL_titles
where titles.title_id = MH_GL_titles.title_id and type = 'movie'
order by primary_title;
