import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

ARN             = config.get('IAM_ROLE', 'ARN')
LOG_DATA        = config.get('S3', 'LOG_DATA')
LOG_JSONPATH    = config.get('S3', 'LOG_JSONPATH')
SONG_DATA       = config.get('S3', 'SONG_DATA')
KEY             = config.get('AWS', 'KEY')
SECRET          = config.get('AWS', 'SECRET')



# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES

staging_events_table_create= (""" 
    CREATE TABLE IF NOT EXISTS staging_events(
    artist varchar, 
    auth varchar,
    firstName varchar,
    gender varchar,
    itemInSession integer,
    lastName varchar,
    length double precision,
    level varchar,
    location varchar,
    method varchar,
    page varchar,
    registration double precision,
    sessionId integer,
    song varchar,
    status integer,
    ts bigint,
    userAgent varchar,
    user_id varchar

    );
""")

staging_songs_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_songs(
        artist_id VARCHAR,
        artist_latitude DOUBLE PRECISION,
        artist_location VARCHAR,
        artist_longitude DOUBLE PRECISION,
        artist_name VARCHAR,
        duration DOUBLE PRECISION,
        num_songs INTEGER,
        song_id VARCHAR,
        title VARCHAR,
        year INTEGER       
    );
""")

songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplays(
        songplay_id INTEGER IDENTITY(0,1) NOT NULL, 
        start_time timestamp NOT NULL SORTKEY,
        user_id VARCHAR,
        level VARCHAR,
        song_id VARCHAR DISTKEY,
        artist_id VARCHAR,
        session_id INTEGER,
        location VARCHAR,
        user_agent VARCHAR    
    );
""")

user_table_create = ("""
    CREATE TABLE IF NOT EXISTS users(
        user_id VARCHAR NOT NULL sortkey,
        first_name VARCHAR, 
        last_name VARCHAR, 
        gender VARCHAR, 
        level VARCHAR
    )
    diststyle all;
""")

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS songs(
        song_id VARCHAR not null sortkey DISTKEY,
        title VARCHAR, 
        artist_id VARCHAR, 
        year INTEGER,
        duration DOUBLE PRECISION
    ); 
""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artists(
        artist_id VARCHAR not null sortkey, 
        name VARCHAR,
        location VARCHAR, 
        lattitude DOUBLE PRECISION,
        longitude DOUBLE PRECISION
    )
    diststyle all;
""")

time_table_create = ("""
    CREATE TABLE IF NOT EXISTS time(
        start_time TIMESTAMP sortkey, 
        hour INT, 
        day INT, 
        week INT, 
        month INT, 
        year INT, 
        weekday INT
    )
    diststyle all;
""")

# STAGING TABLES

staging_events_copy = ("""
    COPY staging_events FROM {}
    CREDENTIALS 'aws_access_key_id={};aws_secret_access_key={}'
    JSON {}
    REGION 'us-west-2';
""").format(LOG_DATA,KEY,SECRET,LOG_JSONPATH)

staging_songs_copy = ("""
    COPY staging_songs(artist_id, artist_latitude, artist_location, artist_longitude, artist_name, duration, song_id, title, year)
    FROM {}
    CREDENTIALS 'aws_access_key_id={};aws_secret_access_key={}'
    JSON 'auto'
    REGION 'us-west-2';
""").format(SONG_DATA,KEY,SECRET)

# FINAL TABLES
    

songplay_table_insert = ("""
    INSERT INTO songplays(start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
    SELECT TIMESTAMP 'epoch' + ev.ts/1000 * interval '1 second' as start_time, 
    ev.user_id,
    ev.level,
    s.song_id,
    s.artist_id,
    ev.sessionId,
    ev.location,
    ev.userAgent
    FROM staging_events ev, staging_songs s
    WHERE ev.page = 'NextSong'
    AND ev.song = s.title
    AND ev.artist = s.artist_name;
""")

user_table_insert = ("""
    INSERT INTO users(user_id, first_name, last_name, gender, level)
    SELECT DISTINCT
    user_id,
    firstName,
    lastName,
    gender,
    level
    FROM staging_events
    WHERE page = 'NextSong';
""")

song_table_insert = ("""
    INSERT INTO songs(song_id, title, artist_id, year, duration)
    SELECT DISTINCT
    song_id,
    title,
    artist_id,
    year,
    duration 
    FROM staging_songs
    WHERE song_id IS NOT NULL;
    
""")

artist_table_insert = ("""
    INSERT INTO artists(artist_id, name, location, lattitude, longitude)
    SELECT DISTINCT
    artist_id,
    artist_name,
    artist_location,
    artist_latitude,
    artist_longitude
    FROM staging_songs
    WHERE artist_id IS NOT NULL;
    
""")

time_table_insert = ("""
    INSERT INTO time(start_time, hour, day,week, month, year, weekday)
    SELECT 
    start_time,
    extract(hour from start_time),
    extract(day from start_time),
    extract(week from start_time), 
    extract(month from start_time),
    extract(year from start_time), 
    extract(dayofweek from start_time)
    FROM songplays;
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
