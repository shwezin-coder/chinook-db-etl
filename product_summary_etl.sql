-- ETL
-- E => extract
-- T => Transform
-- L => Load
-- schedule refresh
use chinook_autoincrement;

-- check variable 
show variables like "%schedule%";

-- event scheduler turn on
set global event_scheduler = on;


-- check artist count per album
create table logs
(
	id int auto_increment Primary Key,
    Checktype text,
    result int,
    created_at datetime default current_timestamp
);

select * from logs;

-- delete event
drop event logs;
delimiter |

create event logs
on schedule
every 1 day
do
begin
insert into logs(`Checktype`,`result`)
select "artist count",count(*)
from(
select count(*)
from album
group by AlbumId
having count(distinct artistId) > 1) at_count;


-- check mediatype count per track
/*select count(TrackId) from
track; 

-- playlistcount per track
select trackId, COUNT(distinct playlistId) as "pcount"
from playlisttrack
group by trackId
having pcount > 1;*/


-- check missing data
insert into logs(`checktype`,`result`)
select "Album_missing",
	sum(case when length(AlbumId) = 0 or AlbumId is null then 1 else 0 end) as "Album_missing"
    -- sum(case when length(MediaTypeId) = 0 or MediaTypeId is null then 1 else 0 end) as "MediaType_missing",
    -- sum(case when length(GenreId) = 0 or GenreId is null then 1 else 0 end) as "Genre_missing"
from track;

insert into logs(`checktype`,`result`)
select "MediaType_missing",
	-- sum(case when length(AlbumId) = 0 or AlbumId is null then 1 else 0 end) as "Album_missing"
    sum(case when length(MediaTypeId) = 0 or MediaTypeId is null then 1 else 0 end) as "MediaType_missing"
    -- sum(case when length(GenreId) = 0 or GenreId is null then 1 else 0 end) as "Genre_missing"
from track;

insert into logs(`checktype`,`result`)
select "Genre_missing",
	-- sum(case when length(AlbumId) = 0 or AlbumId is null then 1 else 0 end) as "Album_missing"
    -- sum(case when length(MediaTypeId) = 0 or MediaTypeId is null then 1 else 0 end) as "MediaType_missing"
    sum(case when length(GenreId) = 0 or GenreId is null then 1 else 0 end)
from track;

-- check missing data for artist
insert into logs(`checktype`,`result`)
select 
		"artist_missing", SUM(case when length(ArtistId) = 0 or ArtistId is null then 1 else 0 end)
from album;

-- check missing data for playlist
/*select
	t.trackId
from track t
left join playlisttrack	pt
on t.TrackId = pt.TrackId
where pt.TrackId is null;

select trackId
from track
where trackId not in( select trackId 
from playlisttrack);

-- check missing data playlist
select playlistId
from playlisttrack
where playlistId not in (select playlistId
from playlist); */

-- check discrepancies for sales between invoice line and invoice
insert into logs(`checktype`,`result`)
select "discrepancies checks for sales between invoice line and invoice", count(*)
from(
select
	i.InvoiceId, Total, sum(UnitPrice * Quantity) as "total_il"
from invoiceline il
join invoice i 
on il.InvoiceId = i.InvoiceId
group by i.InvoiceId, Total
having Total != total_il) discrepancies;

end |

delimiter ;

-- check not sold track 
select trackId
from track
where trackId not in (select trackId
from Invoiceline);

-- unitprice count per track
select trackId, count(distinct UnitPrice) as "up_count"
from invoiceline
group by trackId
having up_count > 1;


/* total sales, total quantity, total invoices, total customers, 
trackId, TrackName, artistName(taylor swift, selena gomez),album, 
media type,genre, playlist,unitprice,milliseconds */

INSERT INTO `Track` (`Name`, `AlbumId`, `MediaTypeId`, `GenreId`, `Composer`, `Milliseconds`, `Bytes`, `UnitPrice`) VALUES
    (N'Miracle', 80, 1, 1, N'Dave Grohl, Taylor Hawkins, Nate Mendel, Chris Shiflett/FOO FIGHTERS', 209684, 6877994, 0.99);

select count(*) from track;
select count(*) from
products_summary;


delimiter |

create event product_summary_etl
on schedule every 1 day
do
begin
	drop table if exists products_summary;
	create table products_summary(
	select 
		t.trackId,
		t.Name as "Track",
		at.Name as "Artist",
		a.Title as "Album",
		mt.Name as "MediaType",
		g.Name as "Genre",
		(select group_concat(distinct Name separator ', ')
		from playlist p
		join playlisttrack pt on
		p.playlistId = pt.PlaylistId 
		where pt.TrackId = t.TrackId) as "Playlists",
		-- group_concat(distinct il(UnitPrice separator ', ') as "changed_UnitPrice",
		sum(il.unitprice * quantity)  as "TotalSales",
		sum(il.quantity) as "TotalQty",
		count(distinct InvoiceLineId) as "TotalInvoices",
		count(distinct CustomerId) as "TotalCustomers",
		t.UnitPrice,
		round(Milliseconds/1000,0) as "seconds"
	from track t
	join album a
	on t.AlbumId = a.AlbumId
	join mediatype mt
	on t.MediaTypeId = mt.MediaTypeId
	join genre g 
	on t.GenreId = g.GenreId
	join artist at
	on a.ArtistId = at.ArtistId
	/* join playlisttrack pt
	on t.TrackId = pt.TrackId
	join playlist p
	on pt.PlaylistId = p.PlaylistId */
	left join invoiceline il
	on t.TrackId = il.TrackId
	left join invoice i
	on il.InvoiceId = i.InvoiceId
	group by t.trackId,
		t.Name,
		at.Name,
		a.Title,
		mt.Name,
		g.Name,
		t.UnitPrice,
		Milliseconds);
end |

delimiter ;






