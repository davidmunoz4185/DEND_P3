import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES

staging_events_table_create = ("""
    CREATE TABLE staging_events(
        artist VARCHAR(500),
        auth VARCHAR(10),
        firstName VARCHAR(25),
        gender CHAR(1),
        itemInSession INT,
        lastName VARCHAR(25),
        length FLOAT,
        level VARCHAR(4),
        location VARCHAR(50),
        method VARCHAR(10),
        page VARCHAR(25),
        registration FLOAT,
        sessionId INT,
        song VARCHAR(350),
        status INT,
        ts VARCHAR(13),
        userAgent VARCHAR(150),
        userId INT
    )
    ;
""")

staging_songs_table_create = ("""
    CREATE TABLE staging_songs
    (
        song_id VARCHAR(18),
        num_songs INT,
        title VARCHAR(350),
        artist_name VARCHAR(500),
        artist_lattitude VARCHAR(25),
        year INT,
        duration FLOAT,
        artist_id VARCHAR(18),
        artist_longitude VARCHAR(25),
        artist_location VARCHAR(1000)
    )
    ;
""")

songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplays
    (
        songplay_id INT IDENTITY(0,1), 
        start_time numeric not null,  
        user_id int not null, 
        level varchar(4) not null,  
        song_id VARCHAR(18) not null distkey, 
        artist_id VARCHAR(18) not null sortkey, 
        session_id int not null, 
        location VARCHAR(1000) not null, 
        user_agent varchar(150) not null
    )
    ;
""")

user_table_create = ("""
    CREATE TABLE IF NOT EXISTS users
    (
        user_id int NOT NULL PRIMARY KEY sortkey,
        first_name varchar(25) not null,
        last_name varchar(25) not null,
        gender char(1) not null,
        level varchar(4) not null
    )
    ;
""")

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS songs
    (
        song_id varchar NOT NULL PRIMARY KEY distkey,
        title varchar(350) not null,
        artist_id varchar(18) not null sortkey,
        year int not null,
        duration float not null
    )
    ;
""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artists
    (
        artist_id varchar(18) NOT NULL distkey, 
        name varchar(500) not null sortkey,
        location varchar(1000) not null,
        lattitude varchar(25),
        longitude varchar(25)
    )
    ;
""")

time_table_create = ("""
    CREATE TABLE IF NOT EXISTS time
    (
        start_time timestamp NOT NULL sortkey,
        hour int not null,
        day int not null,
        week int not null,
        month int not null,
        year int not null,
        weekday varchar not null
    )
    ;
""")

# STAGING TABLES

staging_events_copy = ("""
    copy staging_events from {}
    iam_role {} 
    format as json {}
    ;
""").format(config.get("S3", "LOG_DATA"),
            config.get("IAM_ROLE", "ARN"),
            config.get("S3", "LOG_JSONPATH"))

staging_songs_copy = ("""
    copy staging_songs from {}
    iam_role {} 
    json 'auto' compupdate off region 'us-west-2';
""").format(config.get("S3", "SONG_DATA"),
            config.get("IAM_ROLE", "ARN"))

# FINAL TABLES

songplay_table_insert = ("""
    INSERT INTO songplays
    (
        start_time, user_id, level, song_id, artist_id, session_id, location, user_agent
    )
    SELECT events.ts,
    events.userid,
    events.level,
    songs.song_id,
    songs.artist_id,
    events.sessionid,
    events.location,
    events.useragent
    FROM staging_events events inner join staging_songs songs
    on songs.artist_name = events.artist
    and songs.title = events.song
    WHERE events.page = 'NextSong'
    and songs.artist_name is not null
    and songs.title is not null
    ;
""")

user_table_insert = ("""
    INSERT INTO users
    (
        user_id, first_name, last_name, gender, level
    )
    SELECT userid,
    firstname,
    lastname,
    gender,
    level
    FROM staging_events
    WHERE userid IS NOT NULL
    ;
""")

song_table_insert = ("""
    INSERT INTO songs
    (
        song_id,
        title,
        artist_id,
        year,
        duration
    )
    SELECT song_id,
    title,
    artist_id,
    year,
    duration
    FROM staging_songs
    WHERE song_id IS NOT NULL
    ;
""")

artist_table_insert = ("""
    INSERT INTO artists
    (
        artist_id, 
        name,
        location,
        lattitude,
        longitude
    )
    SELECT artist_id,
    artist_name,
    artist_location,
    artist_lattitude,
    artist_longitude
    FROM staging_songs
    WHERE artist_id IS NOT NULL
    ;
""")

time_table_insert = ("""
    INSERT INTO time
    (
        start_time, 
        hour, 
        day, 
        week, 
        month,
        year,
        weekday
    )
    SELECT 
    TIMESTAMP 'epoch' + ts::bigint/1000 * interval '1 second' as start_time,
    EXTRACT(hour from start_time),
    EXTRACT(day from start_time),
    EXTRACT(week from start_time),
    EXTRACT(month from start_time),
    EXTRACT(year from start_time),
    EXTRACT(weekday from start_time)
    FROM staging_events
    ;
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
