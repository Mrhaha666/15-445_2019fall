with recursive split(word, str) as (
    select '', genres||','
    from titles
    where genres != "\N" 
    union all 
    select substr(str, 0, instr(str, ',')), substr(str, instr(str, ',')+1)
    from split where str !=''
)

select word, count(*) as num
from split
where word != ''
group by word
order by num desc;

