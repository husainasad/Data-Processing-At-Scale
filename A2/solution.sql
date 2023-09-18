-- Drop tables if  already present
DROP TABLE IF EXISTS query1;
DROP TABLE IF EXISTS query2;
DROP TABLE IF EXISTS query3;
DROP TABLE IF EXISTS query4;
DROP TABLE IF EXISTS query5;
DROP TABLE IF EXISTS query6;
DROP TABLE IF EXISTS query7;
DROP TABLE IF EXISTS query8;
DROP TABLE IF EXISTS query9;

-- Query 1
CREATE TABLE query1 AS
SELECT t1.name AS name, count(t2.movieid) AS moviecount
FROM genres AS t1
INNER JOIN hasagenre AS t2 ON t1.genreid=t2.genreid
GROUP BY t1.genreid;

-- Testing query 1
-- SELECT * FROM query1

-- Query 2
CREATE TABLE query2 AS
SELECT t3.name AS name, AVG(t2.rating) AS rating
FROM hasagenre AS t1
INNER JOIN ratings AS t2 ON t1.movieid=t2.movieid
INNER JOIN genres AS t3 ON t1.genreid=t3.genreid
GROUP BY t3.genreid;

-- Testing query 2
-- SELECT * FROM query2

-- Query 3
CREATE TABLE query3 AS
SELECT t1.title AS title, COUNT(t2.rating) as countofratings
FROM movies as t1
INNER JOIN ratings as t2 ON t1.movieid=t2.movieid
GROUP BY t1.movieid
Having COUNT(t2.rating)>=10;

-- Testing query 3
-- SELECT * FROM query3
-- SELECT * FROM query3 where title='Sleepy Hollow (1999)'

-- Query 4
CREATE TABLE query4 AS
SELECT t1.movieid AS movieid, t2.title AS title
FROM hasagenre AS t1
INNER JOIN movies AS t2 ON t1.movieid=t2.movieid
INNER JOIN genres AS t3 ON t1.genreid=t3.genreid
WHERE t3.name='Comedy';

-- Testing query 4
-- SELECT * FROM query4

-- Query 5
CREATE TABLE query5 AS
SELECT t2.title AS title, AVG(t1.rating) AS average
FROM ratings AS t1
INNER JOIN movies AS t2 ON t1.movieid=t2.movieid
GROUP BY t2.title;

-- Testing query 5
-- SELECT * FROM query5
-- SELECT * FROM query5 where title='Where the Heart Is (2000)'

-- Query 6
CREATE TABLE query6 AS
SELECT AVG(t2.rating) AS average
FROM hasagenre AS t1
INNER JOIN ratings AS t2 ON t1.movieid=t2.movieid
INNER JOIN genres AS t3 ON t1.genreid=t3.genreid
WHERE t3.name='Comedy';

-- Testing query 6
-- SELECT * FROM query6

-- Query 7
CREATE TABLE query7 AS
SELECT AVG(t4.rating) AS average
FROM ratings as t4
WHERE t4.movieid IN
	(SELECT t1.movieid
	FROM hasagenre AS t1
	INNER JOIN ratings AS t2 ON t1.movieid=t2.movieid
	INNER JOIN genres AS t3 ON t1.genreid=t3.genreid
	WHERE t3.name='Comedy'
	INTERSECT
	SELECT t1.movieid
	FROM hasagenre AS t1
	INNER JOIN ratings AS t2 ON t1.movieid=t2.movieid
	INNER JOIN genres AS t3 ON t1.genreid=t3.genreid
	WHERE t3.name='Romance');
	
-- Testing query 7
-- SELECT * FROM query7

-- Query 8
CREATE TABLE query8 AS
SELECT AVG(t2.rating)
FROM hasagenre AS t1
INNER JOIN ratings AS t2 ON t1.movieid=t2.movieid
INNER JOIN genres AS t3 ON t1.genreid=t3.genreid
WHERE t3.name='Romance' AND t1.movieid NOT IN
	(SELECT t1.movieid
	FROM hasagenre AS t1
	INNER JOIN ratings AS t2 ON t1.movieid=t2.movieid
	INNER JOIN genres AS t3 ON t1.genreid=t3.genreid
	WHERE t3.name='Comedy');
	
-- Testing query 8
-- SELECT * FROM query8

-- Query 9
-- \set v1 10;

CREATE TABLE query9 AS
SELECT movieid, rating
FROM ratings
WHERE ratings.userid=:v1