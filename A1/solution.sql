CREATE TABLE users(
    userid int PRIMARY KEY,
    name text NOT NULL
);

CREATE TABLE movies(
	movieid int PRIMARY KEY,
	title text
);

CREATE TABLE taginfo(
	tagid int PRIMARY KEY,
	content text
);

CREATE TABLE genres(
	genreid int PRIMARY KEY,
	name text	
);

CREATE TABLE ratings(
	userid int NOT NULL REFERENCES users(userid),
	movieid int NOT NULL REFERENCES movies(movieid),
	rating numeric,
	timestamp bigint,
	PRIMARY KEY(userid, movieid),
	CHECK(rating>=0 AND rating<=5)
);

CREATE TABLE tags(
	userid int NOT NULL REFERENCES users(userid),
	movieid int NOT NULL REFERENCES movies(movieid),
	tagid int NOT NULL REFERENCES taginfo(tagid),
	timestamp bigint,
	PRIMARY KEY(userid, movieid, tagid)
);

CREATE TABLE hasagenre(
	movieid int NOT NULL REFERENCES movies(movieid),
	genreid int NOT NULL REFERENCES genres(genreid),
	PRIMARY KEY(movieid, genreid)
);
