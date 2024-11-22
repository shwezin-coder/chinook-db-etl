use chinook_autoincrement;
select * from invoice;
select * from invoiceline;

-- sales_detail---
-- InvoicelineId, InvoiceId, TrackId,TrackName, UnitPrice, Quantity,TotalSales, InvoiceDate,CustomerId, CustomerName
drop event sales_event;
delimiter |
create event sales_event
on schedule every 1 day
do
begin
drop table if exists sales_detail;
create table sales_detail
select InvoiceLineId,
	   i.InvoiceId,
	   i.CustomerId,
       concat(c.FirstName," ",c.LastName) as "CustomerName",
       InvoiceDate,
       il.TrackId,
       t.Name as "TrackName",
       il.UnitPrice,
       Quantity,
       sum(il.UnitPrice * Quantity) as "TotalSales"
from 
invoiceline il 
join invoice i
on il.InvoiceId = i.InvoiceId
join customer c
on i.CustomerId = c.CustomerId
join Track t
on il.TrackId = t.TrackId
where InvoiceDate <= curdate()
group by il.InvoiceLineId,
	   InvoiceId,
	   CustomerId,
       TrackId,
	   TrackName,
       CustomerName,
       InvoiceDate,
       UnitPrice,
       Quantity;
end |
delimiter ;

-- To Test
INSERT INTO `InvoiceLine` (`InvoiceId`, `TrackId`, `UnitPrice`, `Quantity`) VALUES
    (1, 2, 0.99, 1);
    
select * from sales_detail;
select * from invoiceline;

-- Month over Month
-- totalsales, totaltracks,totalcustomers
with monthly_sales as(
select
	year(InvoiceDate) as "Year",
    month(InvoiceDate) as "Month",
	sum(TotalSales) as "TotalSales",
    count(distinct TrackId) as "TotalTracks",
    count(distinct CustomerId) as "TotalCustomers"
from sales_detail
group by Year,Month
order by Year,Month)
select 
	Year,
    Month,
    TotalSales as "CurrentMonthSales",
    lag(TotalSales) over(order by Year,Month) as "PreviousMonthSales", -- lag function to retrieve previous data
    TotalTracks as "CurrentMonthTracks",
    lag(TotalTracks) over(order by Year,Month) as "PreviousMonthTrack",
    TotalCustomers as "CurrentMonthCustomers",
    lag(TotalCustomers) over(order by Year,Month) as "PreviousMonthCustomers"
from monthly_sales;

-- Cohort---
with customer_purchase_date as(select
	CustomerId,
    Min(InvoiceDate) as "FirstPurchaseDate"
from sales_detail
group by CustomerId),
customer_history as(select
	c.CustomerId,
    date_format(FirstPurchaseDate,'%Y-%m-%01') as "FirstPurchaseDate",-- convert custom format for date
    timestampdiff(month,FirstPurchaseDate,s.invoiceDate) as "Duration" -- date difference depending on different units
from customer_purchase_date c 
join sales_detail s
on c.CustomerId = s.CustomerId)
select
	date_format(FirstPurchaseDate,'%Y-%b') as "Cohort",
	Duration,
	count(distinct CustomerId) as "Total"
from customer_history
group by FirstPurchaseDate,Duration
order by FirstPurchaseDate,Duration;

-- check cohort data 
with customer_test as(select distinct CustomerId
from sales_detail
where InvoiceDate between '2021-01-01' and '2021-01-31') -- betwen for range
select count(distinct c.CustomerId)
from customer_test c 
join sales_detail s
on c.CustomerId = s.CustomerId
and InvoiceDate between '2021-04-01' and '2021-04-30';


    

