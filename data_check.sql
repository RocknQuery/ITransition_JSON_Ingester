select t.*, count(1) over() all_rows_count from tb_first_task_books t;

-- Replaced table with view for data consistency
create or replace view vw_first_task_books_published_by_year as
  with data as
   (select t.id,
           t.title,
           t.author,
           t.genre,
           t.publisher,
           t.year,
           to_number(substr(t.price, 2)) as price,
           substr(t.price, 1, 1) as currency
      from tb_first_task_books t)
  select year publication_year,
         count(1) published_books_that_year,
         round(avg(case
                     when currency = '$' then
                      price
                     else
                      price * 1.2
                   end),
               2) average_price_in_usd
    from data
   group by year
   order by year;

select t.*, count(1) over() all_rows_count
  from vw_first_task_books_published_by_year t;
