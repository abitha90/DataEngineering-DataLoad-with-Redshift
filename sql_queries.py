import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplay"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS song"
artist_table_drop = "DROP TABLE IF EXISTS artist"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= (""" CREATE TABLE "staging_events" (
                                      "artist" VARCHAR (max),
                                      "auth" VARCHAR ,
                                      "firstName" VARCHAR,
                                      "gender" CHAR(3) ,
                                      "iteminSession" SMALLINT,
                                      "lastName" CHARACTER VARYING(20),
                                      "length" DOUBLE PRECISION,
                                      "level" CHAR(5),
                                      "location" TEXT ,
                                      "method" VARCHAR ,
                                      "page" VARCHAR ,
                                      "registration" DOUBLE PRECISION,
                                      "sessionId" SMALLINT,
                                      "song"  TEXT,
                                      "status" SMALLINT,
                                      "ts" BIGINT,
                                      "userAgent" TEXT ,
                                      "userId" SMALLINT
                                      )
""")

staging_songs_table_create = (""" CREATE TABLE "staging_songs" (
                                     "numSongs" INTEGER,
                                     "artistId" VARCHAR,
                                     "artistLattitude" DOUBLE PRECISION,
                                     "artistLongitude" DOUBLE PRECISION,
                                     "artistLocation" VARCHAR,
                                     "artistName" VARCHAR,
                                     "songId" VARCHAR,
                                     "title" VARCHAR(max),
                                     "duration" DOUBLE PRECISION,
                                     "year" SMALLINT
                                     )
""")

songplay_table_create = (""" CREATE TABLE "songplay" (
                                 "songplayId" INT IDENTITY(0,1) ,
                                 "startTime" TIMESTAMP,
                                 "userId" SMALLINT,
                                 "level" CHAR(5),
                                 "songId" VARCHAR,
                                 "artistId" CHARACTER VARYING (25),
                                 "sessionId" BIGINT,
                                 "location" TEXT,
                                 "userAgent" TEXT,
                                 PRIMARY KEY(songplayId)
                                 )
""")

user_table_create = (""" CREATE TABLE "users" (
                              "userId" SMALLINT NOT NULL PRIMARY KEY,
                              "fistName" VARCHAR,
                              "lastName" VARCHAR,
                              "gender" CHAR(3),
                              "level" CHAR(5)
                              )
""")

song_table_create = ("""CREATE TABLE "song" (
                            "songId" VARCHAR PRIMARY KEY,
                            "title" VARCHAR,
                            "artistId" VARCHAR,
                            "year" SMALLINT,
                            "duration" DOUBLE PRECISION
                            )
""")

artist_table_create = (""" CREATE TABLE "artist" (
                               "artistId" VARCHAR NOT NULL PRIMARY KEY,
                               "name" VARCHAR,
                               "location" VARCHAR,
                               "lattitude" DECIMAL,
                               "longitude" DECIMAL
                               )
""")

time_table_create = (""" CREATE TABLE "time" (
                           "startTime" TIMESTAMP NOT NULL PRIMARY KEY,
                           "hour" SMALLINT,
                           "day" CHARACTER VARYING (15),
                           "week" SMALLINT,
                           "month" CHARACTER VARYING (10),
                           "year" SMALLINT,
                           "weekDay" CHARACTER VARYING (10)
                            )
""")

# STAGING TABLES

staging_events_copy = (""" copy staging_events 
                            from {} 
                            credentials 'aws_iam_role={}'
                            timeformat as 'epochmillisecs'
                            format json as {}
                            region 'us-west-2' ;
                           """).format(config.get('S3','LOG_DATA'), 
                                       config.get('IAM_ROLE','ARN'),
                                       config.get('S3','LOG_JSONPATH'))

staging_songs_copy = (""" copy staging_songs (numSongs, artistId, artistLattitude, artistLongitude, artistLocation,
                                            artistName, songId, title, duration, year)
                          from {}
                          credentials 'aws_iam_role={}'
                          json 'auto'
                          region 'us-west-2';
                      """).format(config.get('S3','SONG_DATA'), 
                                  config.get('IAM_ROLE','ARN'))

# FINAL TABLES

songplay_table_insert = (""" INSERT INTO songplay (startTime,
                                                   userId, 
                                                   level,
                                                   songId,
                                                   artistId,
                                                   sessionId,
                                                   location,
                                                   userAgent)
                               SELECT DISTINCT
                                   TIMESTAMP 'epoch' + se.ts/1000 *INTERVAL '1 second' AS startTime,
                                   se.userId AS userId,
                                   se.level AS level,
                                   ss.songId AS songId,
                                   ss.artistId AS artistId,
                                   se.sessionId AS sessionId,
                                   se.location AS location,
                                   se.userAgent AS userAgent
                                FROM staging_events AS se
                                INNER JOIN staging_songs ss on ss.artistName = se.artist AND se.song = ss.title
                                WHERE se.page = 'NextSong';

                                                                  
""")

user_table_insert = (""" INSERT INTO users (userId,
                                            fistName,
                                            lastName,
                                            gender,
                                            level
                                            )
                            SELECT DISTINCT 
                                   se.userId AS userId,
                                   se.firstName AS firstName,
                                   se.lastName AS lastName,
                                   se.gender AS gender,
                                   se.level AS level
                            FROM staging_events AS se
                            WHERE se.page = 'NextSong' AND se.userId is NOT NULL;
""")

song_table_insert = ("""INSERT INTO SONG (songId,
                                           title,
                                           artistId,
                                           year,
                                           duration 
                                           )
                            SELECT DISTINCT
                                  ss.songId AS songId,
                                  ss.title AS title,
                                  ss.artistId AS artistId,
                                  ss.duration AS duration,
                                  ss.year AS year
                             FROM staging_songs AS ss
                             WHERE songId IS NOT NULL;   
 
                                  
""")

artist_table_insert = ("""INSERT INTO artist ( artistId,
                                              name,
                                              location,
                                              lattitude,
                                              longitude
                                             )
                                SELECT DISTINCT
                                      ss.artistId AS artistId,
                                      ss.artistName AS name,
                                      ss.artistLocation AS location,
                                      ss.artistLattitude AS lattitude,
                                      ss.artistLongitude AS longitude
                                 FROM staging_songs AS ss
                                 WHERE ss.artistLocation IS NOT NULL AND 
                                 ss.artistLattitude IS NOT NULL AND 
                                 ss.artistLongitude IS NOT NULL;

                                
""")

time_table_insert = (""" INSERT INTO time (startTime,
                                            hour,
                                            day,
                                            week,
                                            month,
                                            year,
                                            weekDay
                                            )
                         SELECT DISTINCT
                            timestamp 'epoch' + CAST(se.ts AS BIGINT)/1000 * INTERVAL '1 second' AS start_time,
                            EXTRACT(HOUR FROM timestamp 'epoch' + CAST(se.ts AS BIGINT)/1000* INTERVAL '1 second') AS hour,
                            EXTRACT(DAY FROM  timestamp 'epoch' + CAST(se.ts AS BIGINT)/1000* INTERVAL '1 second') AS day,
                            EXTRACT(WEEK FROM  timestamp 'epoch' + CAST(se.ts AS BIGINT)/1000* INTERVAL '1 second') AS week,
                            EXTRACT(MONTH FROM  timestamp 'epoch' + CAST(se.ts AS BIGINT)/1000* INTERVAL '1 second') AS                                       month,
                            EXTRACT(YEAR FROM  timestamp 'epoch' + CAST(se.ts AS BIGINT)/1000* INTERVAL '1 second') AS year,
                            EXTRACT(DOW FROM  timestamp 'epoch' + CAST(se.ts AS BIGINT)/1000* INTERVAL '1 second') AS                                        weekDay
                            FROM staging_events AS se;
                                
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create,songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
