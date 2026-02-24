
  WITH CTE_MY_DATE AS (
    SELECT DATEADD(DAY, SEQ4(), '2004-01-01') AS MY_DATE,
    dateadd(hour,10,getdate()) AS NOW
      FROM TABLE(GENERATOR(ROWCOUNT=>(20000)))  -- Number of days after reference date in previous line
  )
  SELECT DATE(MY_DATE)                                  AS "calendar.date"
        ,YEAR(MY_DATE)                                  AS "calendar.year"
        ,'H' || CEIL(MONTH(MY_DATE) / 6)                AS "calendar.halfyear"
        ,'Q' || QUARTER(MY_DATE)                        AS "calendar.quarter"
        ,TO_CHAR(MY_DATE, 'Mon')                        AS "calendar.month"
        ,MONTH(MY_DATE)                                 AS "calendar.month_num"
        ,WEEK(MY_DATE)                                  AS "calendar.week"
        ,concat(YEAR(MY_DATE), '-', LPAD(MONTH(MY_DATE), 2, '0'))  
                                                        AS "calendar.year_and_month_num"
--        ,concat(YEAR(MY_DATE), '-W', LPAD(WEEK(MY_DATE)::TEXT, 2, '0')) 
--                                                        AS "calendar_year_and_week"
--        ,concat(YEAR(MY_DATE), '-H', CEIL(MONTH(MY_DATE) / 6)::TEXT) 
--                                                        AS "calendar_year_and_half_year"
--        ,concat(YEAR(MY_DATE), '-Q', QUARTER(MY_DATE)::TEXT) 
--                                                        AS "calendar_year_and_quarter"
        ,concat(YEAR(MY_DATE),  '-W', WEEK(MY_DATE))    AS "calendar.year_and_week"
        ,concat(YEAR(MY_DATE),  '-' , TO_CHAR(MY_DATE, 'Mon')) 
                                                        AS "calendar.year_and_month"
        ,concat(YEAR(MY_DATE),  '-H', CEIL(MONTH(MY_DATE) / 6)) 
                                                        AS "calendar.year_and_half_year"
        ,concat(YEAR(MY_DATE),  '-Q', QUARTER(MY_DATE)) AS "calendar.year_and_quarter"
        ,DAYOFYEAR(MY_DATE)                             AS "calendar.day_of_year"
        ,DAYOFMONTH(MY_DATE)                            AS "calendar.day_of_month"
        ,DAYNAME(MY_DATE)                               AS "calendar.day_of_week"

        ,Date(DATE_TRUNC('YEAR', MY_DATE))              AS "calendar.year_start"
        ,Date(DATEADD(DAY, -1, DATEADD(YEAR, 1, DATE_TRUNC('YEAR', MY_DATE))))
                                                        AS "calendar.year_end"
        ,Date(DATE_TRUNC('QUARTER', MY_DATE))           AS "calendar.quarter_start"
        ,Date(DATEADD(DAY, -1, DATEADD(QUARTER, 1, DATE_TRUNC('QUARTER', MY_DATE)))) 
                                                        AS "calendar.quarter_end"
        ,Date(DATE_TRUNC('MONTH', MY_DATE))             AS "calendar.month_start"
        ,Date(DATEADD(DAY, -1, DATEADD(MONTH, 1, DATE_TRUNC('MONTH', MY_DATE)))) 
                                                        AS "calendar.month_end"
        ,Date(DATE_TRUNC('WEEK', MY_DATE))              AS "calendar.week_start"
        ,Date(DATEADD(DAY, 6, DATE_TRUNC('WEEK', MY_DATE)))   
                                                        AS "calendar.week_end"
        ,YEAR(MY_DATE) - 1                              AS "calendar.year_ly" 
        ,CONCAT(YEAR(DATEADD(YEAR, -1, MY_DATE)), '-W', LPAD(WEEK(MY_DATE)::TEXT, 2, '0')) 
                                                        AS "calendar.year_and_week_ly"
        ,CONCAT(YEAR(MY_DATE) - 1, '-', TO_CHAR(MY_DATE, 'Mon')) 
                                                        AS "calendar.year_and_month_ly"
        ,CONCAT(YEAR(MY_DATE) - 1, '-H', CEIL(MONTH(MY_DATE) / 6)::TEXT) 
                                                        AS "calendar.year_and_half_year_ly"
        ,CONCAT(YEAR(MY_DATE) - 1, '-Q', QUARTER(MY_DATE)::TEXT) 
                                                        AS "calendar.year_and_quarter_ly"
    FROM CTE_MY_DATE where MY_DATE <= DATE(DATEADD(DAY, -1, DATE_TRUNC('YEAR',DATEADD(YEAR, 11, getdate()))))