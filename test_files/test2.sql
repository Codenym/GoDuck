with cte as (
    select a, b, c from 
    $firstschema_fitst_table
    where id = 5
)
select a, b, c from 
$someschema_sometable base
left join $anotherschema_another_table j1 on j1.a = base.a
inner join cte on cte.a = base.a
where id = 5
