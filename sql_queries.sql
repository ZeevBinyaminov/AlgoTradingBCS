-- добавить бумагу в алгоритм

TRUNCATE sizes;

INSERT INTO sizes 
VALUES ('LKOH', 50), 
       ('FIVE', 100),
	   ('NVTK', 150);


-- test vwap

INSERT INTO deals (ticker, price, volume)
VALUES ('LKOH', 7000, 50), 
       ('LKOH', 6900, 100),
	   ('LKOH', 6800, 13)

select * from deals

-- calc vwap

SELECT SUM(price * volume) / SUM(volume)
                               FROM deals
                               GROUP BY ticker


-- compare vwap's 

SELECT *, ROUND(vwap_algo/vwap_market - 1, 4) AS difference
FROM results
WHERE volume IS NOT NULL;