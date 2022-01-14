class SqlQueries:
    create_staging_events = ("""
    DROP TABLE IF EXISTS public.staging_events;
    CREATE TABLE public.staging_events (
	artist varchar(256),
	auth varchar(256),
	firstname varchar(256),
	gender varchar(256),
	iteminsession int4,
	lastname varchar(256),
	length numeric(18,0),
	"level" varchar(256),
	location varchar(256),
	"method" varchar(256),
	page varchar(256),
	registration numeric(18,0),
	sessionid int4,
	song varchar(256),
	status int4,
	ts int8,
	useragent varchar(256),
	userid int4
    );""")
    create_staging_songs = ("""
    DROP TABLE IF EXISTS public.staging_songs;
    CREATE TABLE IF NOT EXISTS public.staging_songs (
	num_songs int4,
	artist_id varchar(256),
	artist_name varchar(256),
	artist_latitude numeric(18,0),
	artist_longitude numeric(18,0),
	artist_location varchar(256),
	song_id varchar(256),
	title varchar(256),
	duration numeric(18,0),
	"year" int4
    );""")
    
    create_artists_table = ("""
    DROP TABLE IF EXISTS public.artists;
    CREATE TABLE IF NOT EXISTS public.artists (
	artistid varchar(256) NOT NULL,
	name varchar(256),
	location varchar(256),
	latitude numeric(18,0),
	longitude numeric(18,0)
    );""")
    create_songs_table = ("""
    DROP TABLE IF EXISTS public.songs ;
    CREATE TABLE IF NOT EXISTS public.songs (
	songid varchar(256) NOT NULL,
	title varchar(256),
	artistid varchar(256),
	"year" int4,
	duration numeric(18,0),
	CONSTRAINT songs_pkey PRIMARY KEY (songid)
    );""")
    create_time_table = ("""
    DROP TABLE IF EXISTS public."time";
    CREATE TABLE IF NOT EXISTS public."time" (
	start_time timestamp NOT NULL,
	"hour" int4,
	"day" int4,
	week int4,
	"month" varchar(256),
	"year" int4,
	weekday varchar(256),
	CONSTRAINT time_pkey PRIMARY KEY (start_time)
    );""")
    create_users_table = ("""
    DROP TABLE IF EXISTS public.users;
    CREATE TABLE IF NOT EXISTS public.users (
	userid int4 NOT NULL,
	first_name varchar(256),
	last_name varchar(256),
	gender varchar(256),
	"level" varchar(256),
	CONSTRAINT users_pkey PRIMARY KEY (userid)
    );""")
    create_songplays_table = ("""
    DROP TABLE IF EXISTS public.songplays;
    CREATE TABLE IF NOT EXISTS public.songplays (
	playid varchar(32) NOT NULL,
	start_time timestamp NOT NULL,
	userid int4 NOT NULL,
	"level" varchar(256),
	songid varchar(256),
	artistid varchar(256),
	sessionid int4,
	location varchar(256),
	user_agent varchar(256),
	CONSTRAINT songplays_pkey PRIMARY KEY (playid)
    );""")
    songplay_table_insert = ("""
    INSERT INTO songplays (playid, start_time, userid, level, songid, artistid, sessionid, location, user_agent)
        SELECT
                md5(events.sessionId || events.ts) playid,
                events.start_time, 
                events.userid, 
                events.level, 
                songs.song_id as songid, 
                songs.artist_id as artistid, 
                events.sessionid, 
                events.location, 
                events.useragent as user_agent
                FROM (SELECT TIMESTAMP 'epoch' + ts/1000 * interval '1 second' AS start_time, *
            FROM staging_events
            WHERE page='NextSong') events
            LEFT JOIN staging_songs songs
            ON events.song = songs.title
                AND events.artist = songs.artist_name
                AND events.length = songs.duration
    """)

    user_table_insert = ("""
    INSERT INTO users (userid, first_name, last_name, gender, level)
        SELECT distinct userid, 
                        firstname as first_name, 
                        lastname as last_name, 
                        gender, 
                        level
        FROM staging_events
        WHERE page='NextSong' AND userid NOT IN (SELECT DISTINCT userid FROM users);
    """)

    song_table_insert = ("""
    INSERT INTO songs (songid, title, artistid, year, duration)
        SELECT distinct song_id as songid, 
                        title, 
                        artist_id as artistid, 
                        year, 
                        duration
        FROM staging_songs 
        WHERE song_id NOT IN (SELECT DISTINCT songid from songs);
    """)

    artist_table_insert = ("""
    INSERT INTO artists (artistid, name, location, latitude, longitude)
        SELECT distinct artist_id as artistid, 
                        artist_name as name, 
                        artist_location as location, 
                        artist_latitude as latitude, 
                        artist_longitude as longitude
        FROM staging_songs
        WHERE artist_id NOT IN (SELECT DISTINCT artistid from artists);
    """)

    time_table_insert = ("""
    INSERT INTO time (start_time, hour, day, week, month, year, weekday)
        SELECT start_time, extract(hour from start_time), extract(day from start_time), extract(week from start_time), 
               extract(month from start_time), extract(year from start_time), extract(dayofweek from start_time)
        FROM songplays WHERE start_time NOT IN (SELECT DISTINCT start_time from time)
    """)
    check_queries=[
        {"sql_stmt": "SELECT * FROM users WHERE userid IS NULL" },
        {"sql_stmt": "SELECT * FROM songs WHERE songid IS NULL" },
        {"sql_stmt": "SELECT * FROM artists WHERE artistid IS NULL" },
        {"sql_stmt": "SELECT * FROM time WHERE start_time IS NULL"},
        {"sql_stmt": "SELECT * FROM songplays WHERE playid IS NULL" }]
    tables = ['users','songs','artists','time','songplays']