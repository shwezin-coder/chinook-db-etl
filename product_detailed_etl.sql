use chinook;
-- event scheduler on
set global event_scheduler = on;

-- create data quality issues logs
create table data_quality_issues_logs(
log_id int auto_increment Primary Key,
check_type text,
result int,
created_date timestamp default current_timestamp);

-- for checking data quality issues 
DELIMITER //

CREATE EVENT data_quality_issues_logs
ON SCHEDULE EVERY 1 day STARTS '2024-11-18 05:47:00'
DO
BEGIN
    -- Check not found track in invoiceline
    INSERT INTO data_quality_issues_logs(check_type, result)
    SELECT "track not found in invoiceline", COUNT(il.TrackId)
    FROM invoiceline il
    LEFT JOIN track t ON il.TrackId = t.TrackId
    WHERE t.TrackId IS NULL;

    -- Check artist count per album
    INSERT INTO data_quality_issues_logs(check_type, result)
    SELECT "artist count per album", COUNT(1)
    FROM (
        SELECT AlbumId, COUNT(DISTINCT artistId) AS total_artists
        FROM album
        GROUP BY AlbumId
        HAVING total_artists > 1
    ) AS artistcount;

    -- Check not found invoice in invoiceline
    INSERT INTO data_quality_issues_logs(check_type, result)
    SELECT "not found invoice in invoiceline", COUNT(il.invoiceId)
    FROM invoiceline il
    LEFT JOIN invoice i ON il.invoiceId = i.invoiceId
    WHERE i.invoiceId IS NULL;

    -- Check discrepancies between invoice and invoiceline
    INSERT INTO data_quality_issues_logs(check_type, result)
    SELECT 'check discrepancies between invoice and invoiceline', COUNT(1)
    FROM (
        SELECT i.invoiceId, i.Total, SUM(il.UnitPrice * il.Quantity) AS total_calculated
        FROM invoiceline il
        JOIN invoice i ON il.InvoiceId = i.InvoiceId
        GROUP BY i.invoiceId, i.Total
        HAVING i.Total != SUM(il.UnitPrice * il.Quantity)
    ) AS discrepancies;
END //

DELIMITER ;


-- retrieve from data quality issues logs
select * from data_quality_issues_logs;

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
