-- Drop tables if  already present
DROP TABLE IF EXISTS ratings;
DROP TABLE IF EXISTS tags;
DROP TABLE IF EXISTS hasagenre;
DROP TABLE IF EXISTS users;
DROP TABLE IF EXISTS movies;
DROP TABLE IF EXISTS taginfo;
DROP TABLE IF EXISTS genres;

-- Create tables
CREATE TABLE users(
    userid int PRIMARY KEY,
    name text NOT NULL
);

CREATE TABLE movies(
	movieid int PRIMARY KEY,
	title text NOT NULL
);

CREATE TABLE taginfo(
	tagid int PRIMARY KEY,
	content text NOT NULL
);

CREATE TABLE genres(
	genreid int PRIMARY KEY,
	name text NOT NULL
);

CREATE TABLE ratings(
	userid int REFERENCES users(userid),
	movieid int REFERENCES movies(movieid),
	rating numeric NOT NULL,
	timestamp bigint NOT NULL,
	PRIMARY KEY(userid, movieid),
	CHECK(rating>=0 AND rating<=5)
);

CREATE TABLE tags(
	userid int REFERENCES users(userid),
	movieid int REFERENCES movies(movieid),
	tagid int REFERENCES taginfo(tagid),
	timestamp bigint NOT NULL,
	PRIMARY KEY(userid, movieid, tagid)
);

CREATE TABLE hasagenre(
	movieid int REFERENCES movies(movieid),
	genreid int REFERENCES genres(genreid),
	PRIMARY KEY(movieid, genreid)
);


-- ***Query for data insert***

\copy users from 'D:\ASU_2023_Fall\Data Processing at Scale\Assignment 1\users.dat' DELIMITERS '%';
\copy movies from 'D:\ASU_2023_Fall\Data Processing at Scale\Assignment 1\movies.dat' DELIMITERS '%';
\copy taginfo from 'D:\ASU_2023_Fall\Data Processing at Scale\Assignment 1\taginfo.dat' DELIMITERS '%';
\copy genres from 'D:\ASU_2023_Fall\Data Processing at Scale\Assignment 1\genres.dat' DELIMITERS '%';
\copy ratings from 'D:\ASU_2023_Fall\Data Processing at Scale\Assignment 1\ratings.dat' DELIMITERS '%';
\copy tags from 'D:\ASU_2023_Fall\Data Processing at Scale\Assignment 1\tags.dat' DELIMITERS '%';
\copy hasagenre from 'D:\ASU_2023_Fall\Data Processing at Scale\Assignment 1\hasagenre.dat' DELIMITERS '%';

-- ***Testing***

-- Normal Data Insertion
INSERT INTO users
VALUES (10000, 'newuser');

INSERT INTO movies
VALUES (65135, 'Blue Beetle');

INSERT INTO taginfo
VALUES (3280, 'mexican comedy');

INSERT INTO genres
VALUES (19, 'loco');

INSERT INTO ratings
VALUES (10000, 65135, 4, 1162164173);

INSERT INTO tags
VALUES (10000, 65135, 19, 1162164173);

INSERT INTO hasagenre
VALUES (65135, 19);

-- Non-existent Foreign Key insertion
INSERT INTO ratings
VALUES (10000, 65137, 4, 1162164173);

INSERT INTO tags
VALUES (10001, 65135, 19, 1162164173);

INSERT INTO hasagenre
VALUES (65137, 19);

-- Duplicate rating insertion
INSERT INTO ratings
VALUES (10000, 65135, 3.5, 1162164177);

-- Wrong genre id insertion
INSERT INTO hasagenre
VALUES (65135, 20);

-- Inserting rating larger than 5
INSERT INTO ratings
VALUES (9999, 65135, 6, 1162164173);

-- Inserting movie with no genre
INSERT INTO hasagenre
VALUES (65135, NULL);

SELECT * from hasagenre
WHERE movieid=65135

-- Adding multiple tags to same movie
INSERT INTO tags
VALUES (10000, 65135, 3280, 1162164173);

INSERT INTO tags
VALUES (10000, 65135, 3200, 1162164173);

SELECT * from tags
WHERE movieid=65135