 /*LOAD Distinct 
        DATE([$(_DateField)]) 																					as [$(_CalendarPrefix)date],
        YEAR([$(_DateField)]) 																					as [$(_CalendarPrefix)year],
        'H' & CEIL(MONTH([$(_DateField)]) / 6 ) 																as [$(_CalendarPrefix)halfyear],
        'Q' & CEIL(MONTH([$(_DateField)]) / 3 ) 																as [$(_CalendarPrefix)quarter],
        MONTH([$(_DateField)])																					as [$(_CalendarPrefix)month],
        NUM(MONTH([$(_DateField)]), '00') 																		as [$(_CalendarPrefix)month_num],
        WEEK([$(_DateField)]) 																					as [$(_CalendarPrefix)week],
//         WEEKYEAR([$(_DateField)]) & '-W' & NUM(WEEK([$(_DateField)]), '00')										as [$(_CalendarPrefix)year_and_week],
        YEAR([$(_DateField)]) & '-' & NUM(MONTH([$(_DateField)]), '00')					 						as [$(_CalendarPrefix)year_and_month_num],
//         YEAR([$(_DateField)]) & '-H' & NUM(CEIL(MONTH([$(_DateField)]) / 6 ), '0') 								as [$(_CalendarPrefix)year_and_half_year],
//         YEAR([$(_DateField)]) & '-Q' & NUM(CEIL(MONTH([$(_DateField)]) / 3 ), '0') 								as [$(_CalendarPrefix)year_and_quarter],
        DUAL(WEEKYEAR([$(_DateField)]) & '-W' & NUM(WEEK([$(_DateField)]), '00'),YEAR([$(_DateField)]) * 100 + WEEK([$(_DateField)]))	
        																										as [$(_CalendarPrefix)year_and_week],
        DUAL(YEAR([$(_DateField)]) & '-' & MONTH([$(_DateField)]),YEAR([$(_DateField)]) * 100 + MONTH([$(_DateField)]))					
        																										as [$(_CalendarPrefix)year_and_month],
        DUAL(YEAR([$(_DateField)]) & '-H' & NUM(CEIL(MONTH([$(_DateField)]) / 6 ), '0'),YEAR([$(_DateField)]) * 100 + CEIL(MONTH([$(_DateField)])/6)) 								
        																										as [$(_CalendarPrefix)year_and_half_year],
        DUAL(YEAR([$(_DateField)]) & '-Q' & NUM(CEIL(MONTH([$(_DateField)]) / 3 ), '0'),YEAR([$(_DateField)]) * 100 + CEIL(MONTH([$(_DateField)])/3)) 								
        																										as [$(_CalendarPrefix)year_and_quarter],                                                                                                               
        
        DayNumberOfYear([$(_DateField)]) 																		as [$(_CalendarPrefix)day_of_year],
        Day([$(_DateField)]) 																					as [$(_CalendarPrefix)day_of_month],
        WeekDay([$(_DateField)])  																				as [$(_CalendarPrefix)day_of_week],

        DATE(FLOOR(YEARSTART([$(_DateField)]))) 																as [$(_CalendarPrefix)year_start],
        DATE(FLOOR(YEAREND([$(_DateField)]))) 																	as [$(_CalendarPrefix)year_end],
        DATE(FLOOR(QUARTERSTART([$(_DateField)]))) 																as [$(_CalendarPrefix)quarter_start],
        DATE(FLOOR(QUARTEREND([$(_DateField)]))) 																as [$(_CalendarPrefix)quarter_end],
        DATE(FLOOR(MONTHSTART([$(_DateField)]))) 																as [$(_CalendarPrefix)month_start],
        DATE(FLOOR(MONTHEND([$(_DateField)]))) 																	as [$(_CalendarPrefix)month_end],
        DATE(FLOOR(WEEKSTART([$(_DateField)]))) 																as [$(_CalendarPrefix)week_start],
        DATE(FLOOR(WEEKEND([$(_DateField)]))) 																	as [$(_CalendarPrefix)week_end],
        
        YEAR([$(_DateField)])-1 																				as [$(_CalendarPrefix)year_ly],
//         WEEKYEAR([$(_DateField)])-1 & '-W' & NUM(WEEK([$(_DateField)]), '00')									as [$(_CalendarPrefix)year_and_week_ly],
//         YEAR([$(_DateField)])-1 & '-' & MONTH([$(_DateField)])													as [$(_CalendarPrefix)year_and_month_ly],
//         YEAR([$(_DateField)])-1 & '-H' & NUM(CEIL(MONTH([$(_DateField)]) / 6 ), '0') 							as [$(_CalendarPrefix)year_and_half_year_ly],
//         YEAR([$(_DateField)])-1 & '-Q' & NUM(CEIL(MONTH([$(_DateField)]) / 3 ), '0') 							as [$(_CalendarPrefix)year_and_quarter_ly] 
        DUAL(WEEKYEAR([$(_DateField)])-1 & '-W' & NUM(WEEK([$(_DateField)]), '00'),(YEAR([$(_DateField)])-1) * 100 + WEEK([$(_DateField)]))									
        																										as [$(_CalendarPrefix)year_and_week_ly],
        DUAL(YEAR([$(_DateField)])-1 & '-' & MONTH([$(_DateField)]),(YEAR([$(_DateField)])-1) * 100 + MONTH([$(_DateField)]))													
        																										as [$(_CalendarPrefix)year_and_month_ly],
        DUAL(YEAR([$(_DateField)])-1 & '-H' & NUM(CEIL(MONTH([$(_DateField)]) / 6 ), '0'),(YEAR([$(_DateField)])-1) * 100 + CEIL(MONTH([$(_DateField)])/6)) 							
        																										as [$(_CalendarPrefix)year_and_half_year_ly],
        DUAL(YEAR([$(_DateField)])-1 & '-Q' & NUM(CEIL(MONTH([$(_DateField)]) / 3 ), '0'),(YEAR([$(_DateField)])-1) * 100 + CEIL(MONTH([$(_DateField)])/3)) 							
        																										as [$(_CalendarPrefix)year_and_quarter_ly] 
   	;*/
 
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