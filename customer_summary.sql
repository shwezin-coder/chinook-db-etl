show variables like '%event%';
set global event_scheduler = "on";
-- check employee of customer exists in employee
select * from customer
where SupportRepId not in(select EmployeeId
from employee);

-- check customerID of customer exist in invoice
select * from customer
where customerId not in( select customerId from invoice);

-- check quantity on invoiceline
select distinct quantity
from invoiceline;

/* customerId, Name, Company, address, city,state,country, postalcode,fax,email,
SupportRepId, EmployeeName,
customer status => active, churn before 6 months than current date
totaltracks,totalinvoice,totalsales,totalquantity,firstpurchaseddate,lastpurchasedate
*/

-- customer summary
-- with customer_summary as(
drop event customers_summary;
delimiter |
create event customers_summary
on schedule every 1 day
do
begin
drop table if exists customers_summary;
create table customers_summary
select
	c.CustomerId,
    concat(c.FirstName," ",c.LastName) as "CustomerName",
    Company,
    c.Address,
    c.City,
    c.State,
    c.Country,
    c.PostalCode,
    c.Phone,
    c.Fax,
    c.Email,
    case when max(i.InvoiceDate) >= date_sub(curdate(),interval 6 month) then "active" else "churn" end as "status",
    SupportRepId as "EmployeeId",
    concat(c.FirstName," ",c.LastName) as "EmployeeName",
    count(distinct i.InvoiceId) as "TotalInvoices",
    count(distinct il.TrackId) as "TotalTracks",
    sum(il.UnitPrice * il.Quantity) as "TotalSales",
    min(i.InvoiceDate) as "FirstPurchaseDate",
    max(i.InvoiceDate) as "LastPurchaseDate"
from
customer c
join employee e
on c.SupportRepId = e.EmployeeId
left join invoice i
on c.CustomerId = i.CustomerId
left join invoiceline il
on i.InvoiceId = il.InvoiceId
where i.CustomerID is null or i.InvoiceDate <= curdate()
group by
c.CustomerId,
    concat(c.FirstName," ",c.LastName),
    Company,
    c.Address,
    c.City,
    c.State,
    c.Country,
    c.PostalCode,
    c.Phone,
    c.Fax,
    c.Email,
    SupportRepId,
    concat(c.FirstName," ",c.LastName);
end |
delimiter ;

-- to check
select * from customer;
select * from customers_summary;

-- insert customer
INSERT INTO `Customer` (`FirstName`, `LastName`, `Company`, `Address`, `City`, `State`, `Country`, `PostalCode`, `Phone`, `Fax`, `Email`, `SupportRepId`) VALUES
    (N'Luís', N'Gonçalves', N'Embraer - Empresa Brasileira de Aeronáutica S.A.', N'Av. Brigadeiro Faria Lima, 2170', N'São José dos Campos', N'SP', N'Brazil', N'12227-000', N'+55 (12) 3923-5555', N'+55 (12) 3923-5566', N'luisg@embraer.com.br', 3);


/* )select 
	sum(il.UnitPrice * il.Quantity) as "iTotal",
	cs.TotalSales as "csTotal",
    cs.TotalTracks,
    cs.TotalInvoices,
    i.CustomerId    
from invoice i 
join
customer_summary cs
on i.CustomerId = cs.CustomerId
join
invoiceline il
on i.InvoiceId = il.InvoiceId
where i.InvoiceDate < curdate()
group By i.CustomerId,csTotal,cs.TotalInvoices,cs.TotalTracks
having iTotal = csTotal and count(distinct i.InvoiceId) = cs.TotalInvoices and count(distinct il.TrackId) = TotalTracks;
*/

select sum(UnitPrice * Quantity) from 
invoice i 
join invoiceline il
on i.InvoiceId = il.InvoiceId
where CustomerId = 4;
