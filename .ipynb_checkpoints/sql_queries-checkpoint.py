import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES


staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS  staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS  songplays"
user_table_drop = "DROP TABLE IF EXISTS  users "
song_table_drop = "DROP TABLE IF EXISTS  songs "
artist_table_drop = "DROP TABLE IF EXISTS  artists"
time_table_drop = "DROP TABLE IF EXISTS  time"

# STAGING TABLES
staging_events_table_create= ("""
CREATE TABLE staging_events(
  artist varchar,
  auth varchar(256),
  firstName varchar(256),
  gender varchar(256),
  itemInSession int,
  lastName varchar(256),
  length float,
  level varchar(256),
  location text,
  method varchar(256),
  page varchar(256),
  registration decimal,
  sessionId int,
  song  varchar(255),
  status int,
  ts timestamp,
  userAgent text,
  user_id int
  );
""")

staging_songs_table_create = ("""
CREATE TABLE staging_songs(
    song_id varchar(256)  PRIMARY KEY,
    num_songs int,
    artist_id varchar(256), 
    artist_latitude decimal,
    artist_longitude decimal,
    artist_location varchar(256) ,
    artist_name varchar(256) ,
    title varchar(256) ,
    duration decimal, 
    year smallint );
""")

user_table_create = ("""
CREATE TABLE  IF NOT EXISTS  users
(
    user_id int  PRIMARY KEY,
    first_name VARCHAR(50) NOT NULL, 
    last_name VARCHAR(50) NOT NULL, 
    gender  CHARACTER(1) NOT NULL, 
    level VARCHAR(10) 
);
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS  songs
(
    song_id  VARCHAR  PRIMARY KEY,
    title VARCHAR, 
    artist_id VARCHAR, 
    year INT,
    duration  float );
""")

artist_table_create = ("""
CREATE TABLE  IF NOT EXISTS artists
(
    artist_id  VARCHAR(50)  PRIMARY KEY,
    name VARCHAR NOT NULL, 
    location  VARCHAR, 
    latitude float,
    longitude float
);
""")

#     start_time, hour, day, week, month, year, weekday

time_table_create = ("""
CREATE TABLE  IF  NOT EXISTS  time
(
    start_time timestamp PRIMARY KEY,
    hour INT,
    day INT,
    week INT,
    month INT,
    year INT,
    weekday INT
);
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays 
(   ID  BIGINT IDENTITY(0,1),
    songplay_id int ,
    start_time timestamp      REFERENCES  time(start_time)  NOT NULL,
    user_id  int     REFERENCES  users(user_id) NOT NULL,
    level VARCHAR(10) ,
    song_id VARCHAR(50)   REFERENCES  songs(song_id)  ,
    artist_id VARCHAR(50)   REFERENCES  artists(artist_id)  ,
    session_id INT NOT NULL,
    location  VARCHAR(50) ,
    user_agent TEXT );
""")

# LOAD DATA FROM S3 to STAGEING TABLES
staging_events_copy = ("""
copy staging_events from {}
credentials  {}
format as json {}
    region 'us-west-2'
    timeformat as 'epochmillisecs';
   
""").format(config['S3']['LOG_DATA'],config['IAM_ROLE']['ARN'],config['S3']['LOG_JSONPATH'])

staging_songs_copy = ("""
   
copy staging_songs from {}
credentials  {}
format as json 'auto'
    region 'us-west-2'
    ;

""").format(config['S3']['SONG_DATA'],config['IAM_ROLE']['ARN'])

# INSERT DATA FROM STAGING Tables STAR SCHEMA

user_table_insert = ("""
insert into users (    user_id,first_name , last_name , gender  , level  )
select distinct user_id,firstName,lastName,gender,level from staging_events WHERE user_id IS NOT NULL;
""")


song_table_insert = ("""
insert into songs   
(song_id ,title,artist_id ,year ,duration)
select distinct song_id ,title,artist_id ,year ,duration
from staging_songs  WHERE song_id IS NOT NULL;
""")

artist_table_insert = ("""
insert into artists (  artist_id  ,name ,location,latitude ,longitude )    
select distinct artist_id  ,    artist_name ,     artist_location  ,     artist_latitude ,    artist_longitude
from staging_songs  WHERE artist_id IS NOT NULL;
""")

time_table_insert = ("""
insert into time
(
    start_time   ,
    hour ,
    day ,
    week ,
    month ,
    year ,
    weekday 
) select distinct ts,
Extract(Hour from ts),
Extract(day from ts),
Extract(week from ts),
Extract(month from ts),
Extract(year from ts),
Extract(weekday from ts)
 FROM staging_events where page  =  'NextSong';
""")
# FINAL TABLES

songplay_table_insert = ("""
insert into songplays   
(
    start_time ,
    user_id  ,
    level  ,
    song_id  ,
    artist_id ,
    session_id ,
    location ,
    user_agent  )
    select distinct  staging_events.ts,
    staging_events.user_id      ,
    staging_events.level        ,
    staging_songs.song_id      ,
    staging_songs.artist_id    ,
    staging_events.sessionId   , 
    staging_events.location     ,
    staging_events.userAgent 
    from staging_events    JOIN staging_songs   ON (staging_events.song = staging_songs.title AND staging_events.artist = staging_songs.artist_name)
    AND staging_events.page  =  'NextSong'
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, user_table_create, song_table_create, artist_table_create, time_table_create,songplay_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop,  user_table_drop, song_table_drop, artist_table_drop, time_table_drop,songplay_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [user_table_insert, song_table_insert, artist_table_insert, time_table_insert,songplay_table_insert]
