use chinook;

-- check not found track in invoiceline
select TrackId
from invoiceline
where TrackId not in(select TrackId
from track);

-- check artist count per album
select AlbumId, COUNT(distinct artistId) as total_artists
from album 
group by 1
having total_artists > 1;


-- check not found invoice in invoiceline
select * from
invoiceline
where invoiceId NOT IN(select invoiceId
from invoice);

-- check total on invoice and unitprice * quantity from invoiceLine
select i.invoiceId,i.Total,sum(il.UnitPrice * il.Quantity)
from invoiceline il JOIN
invoice i
on il.InvoiceId = i.InvoiceId
group by i.invoiceId,i.Total
having i.Total != sum(il.UnitPrice * il.Quantity);

-- event scheduler on
set global event_scheduler = on;

-- run by schedule for product_rpt_detail
DELIMITER //
CREATE EVENT product_rpt_detail
ON SCHEDULE EVERY 1 day
STARTS '2024-11-18 03:25:00'
DO
BEGIN
    DROP TABLE IF EXISTS product_detail;
    
    CREATE TABLE product_detail AS
    SELECT
        t.TrackId,
        il.invoiceLineId,
        SUM(i.Total) AS "Total Sales",
        SUM(il.Quantity) AS "Total Quantity",
        COUNT(DISTINCT i.CustomerId) AS "Total Customers",
        COUNT(DISTINCT il.InvoiceLineId) AS "Total Invoices",
        t.Name AS Track,
        ab.Title AS Album,
        p.Name AS Playlist,
        mt.Name AS MediaType,
        g.Name AS Genre,
        t.UnitPrice,
        t.Bytes,
        ROUND(t.Milliseconds / 1000, 0) AS "seconds"
    FROM track t
    LEFT JOIN album ab ON t.AlbumId = ab.AlbumId
    JOIN MediaType mt ON t.MediaTypeId = mt.MediaTypeId
    JOIN Genre g ON t.GenreId = g.GenreId
    JOIN playlisttrack pt ON pt.TrackId = t.TrackId
    JOIN playlist p ON p.PlaylistId = pt.PlaylistId
    JOIN invoiceline il ON il.TrackId = t.TrackId
    JOIN invoice i ON il.InvoiceId = i.InvoiceId
    GROUP BY 
        t.TrackId,
        il.invoiceLineId,
        t.Name,
        ab.Title,
        p.Name,
        mt.Name,
        g.Name,
        t.UnitPrice,
        t.Bytes,
        t.Milliseconds;
END;
//

DELIMITER ;
