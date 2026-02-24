
 
  WITH CTE_MY_DATE AS (
    SELECT DATEADD(DAY, SEQ4(), DATE_TRUNC('YEAR', dateadd(year,-20,getdate()))) AS MY_DATE,
    dateadd(hour,10,getdate()) AS NOW
      FROM TABLE(GENERATOR(ROWCOUNT=>(20000)))  -- Number of days after reference date in previous line
  )
  SELECT DATE(MY_DATE)                                  AS "date"
    FROM CTE_MY_DATE where MY_DATE <= to_date(concat(left(dateadd(year, 10,dateadd(hour,10,getdate())), 4), '-12-31'))
    order by 1 asc