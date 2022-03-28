class SqlQueries:
    
    songplay_table_query = ("""
        SELECT
                events.start_time, 
                events.userid, 
                events.level, 
                songs.song_id, 
                songs.artist_id, 
                events.sessionid, 
                events.location, 
                events.useragent
                FROM (SELECT TIMESTAMP 'epoch' + ts/1000 * interval '1 second' AS start_time, *
            FROM staging_events
            WHERE page='NextSong') events
            LEFT JOIN staging_songs songs
            ON events.song = songs.title
                AND events.artist = songs.artist_name
                AND events.length = songs.duration
    """)

    user_table_query = ("""
        SELECT distinct userid, firstname, lastname, gender, level
        FROM staging_events
        WHERE page='NextSong'
    """)

    song_table_query = ("""
        SELECT distinct song_id, title, artist_id, year, duration
        FROM staging_songs
    """)

    artist_table_query = ("""
        SELECT distinct artist_id, artist_name, artist_location, artist_latitude, artist_longitude
        FROM staging_songs
    """)

    time_table_query = ("""
        SELECT start_time, extract(hour from start_time), extract(day from start_time), extract(week from start_time), 
               extract(month from start_time), extract(year from start_time), extract(dayofweek from start_time)
        FROM songplays
    """)

    

    truncate_sql = ("""
        TRUNCATE TABLE {}
    """)

    copy_sql = ("""
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        JSON '{}'
    """)
    
    '''
    copy_sql = ("""
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        IGNOREHEADER {}
        JSON '{}'
        DELIMITER '{}'
    """)
    '''

    staging_events_table_create= ("""
        
        CREATE TABLE IF NOT EXISTS staging_events 
        (
        artist VARCHAR,
        auth VARCHAR,
        firstName VARCHAR,
        gender VARCHAR,
        itemInSession INT,
        lastName VARCHAR,
        length NUMERIC,
        level VARCHAR,
        location VARCHAR,
        method VARCHAR,
        page VARCHAR,
        registration NUMERIC,
        sessionId INT,
        song VARCHAR,
        status INT,
        ts NUMERIC,
        userAgent VARCHAR,
        userId INT
        );
    """)

    staging_songs_table_create = ("""
        
        CREATE TABLE IF NOT EXISTS staging_songs
        (       
            num_songs INT,
            artist_id VARCHAR,
            artist_latitude NUMERIC,
            artist_longitude NUMERIC,
            artist_location VARCHAR,
            artist_name VARCHAR,
            song_id VARCHAR,
            title VARCHAR,
            duration NUMERIC,
            year INT
        )
    """)
        
    songplay_table_create = ("""
        
        CREATE TABLE IF NOT EXISTS songplays  
        (
            songplay_id INT GENERATED BY DEFAULT AS IDENTITY(0,1) PRIMARY KEY sortkey distkey, 
            start_time TIMESTAMP NOT NULL REFERENCES time (start_time), 
            user_id INT NOT NULL REFERENCES users (user_id), 
            level VARCHAR, 
            song_id VARCHAR, 
            artist_id VARCHAR, 
            session_id INT, 
            location VARCHAR, 
            user_agent VARCHAR
        ) ;
    ;""")

    user_table_create = ("""
        
        CREATE TABLE IF NOT EXISTS users 
        (
            user_id INT PRIMARY KEY sortkey distkey, 
            first_name VARCHAR, 
            last_name VARCHAR, 
            gender VARCHAR, 
            level VARCHAR
        ) ;
    ;""")

    song_table_create = ("""
        
        CREATE TABLE IF NOT EXISTS songs 
        (
            song_id VARCHAR PRIMARY KEY sortkey distkey, 
            title VARCHAR, 
            artist_id VARCHAR NOT NULL, 
            year INT, 
            duration NUMERIC
        ) ;
    """)

    artist_table_create = ("""
        
        CREATE TABLE IF NOT EXISTS artists 
        (
            artist_id VARCHAR PRIMARY KEY sortkey distkey, 
            name VARCHAR, 
            location VARCHAR, 
            latitude NUMERIC, 
            longitude NUMERIC
        );
    """)

    time_table_create = ("""
        
        CREATE TABLE IF NOT EXISTS time 
        (
            start_time TIMESTAMP PRIMARY KEY sortkey distkey, 
            hour INT, 
            day INT, 
            week INT, 
            month INT, 
            year INT, 
            weekday INT
        ) ;
    """)

    songplay_table_insert = ("""
        INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent) 
    """)

    user_table_insert = ("""
        INSERT INTO users (user_id, first_name, last_name, gender, level) 
    """)

    song_table_insert = ("""
        INSERT INTO songs (song_id, title, artist_id, year, duration) 
    """)

    artist_table_insert = ("""
        INSERT INTO artists (artist_id, name, location, latitude, longitude) 
    """)

    time_table_insert = ("""
        INSERT INTO time (start_time, hour, day, week, month, year, weekday) 
    """)

    all_table_name = ['staging_events', 'staging_songs', 'users', 'songs', 'artists', 'time', 'songplays']
    create_all_table = [staging_events_table_create, staging_songs_table_create, user_table_create, song_table_create, artist_table_create, time_table_create, songplay_table_create]

    
    songplay_check_quality = ("""
        SELECT COUNT(*) FROM songplays WHERE user_id is null
    """)
    
    user_check_quality = ("""
        SELECT COUNT(*) FROM users WHERE user_id is null
    """)
    
    song_check_quality = ("""
        SELECT COUNT(*) FROM songs WHERE song_id is null
    """)
    
    artist_check_quality = ("""
        SELECT COUNT(*) FROM artists WHERE artist_id is null
    """)
    
    time_check_quality = ("""
        SELECT COUNT(*) FROM time WHERE start_time is null
    """)
    
    all_table_check_quality = ['users', 'songs', 'artists', 'time', 'songplays']
    check_quality_stmts = [user_check_quality, song_check_quality, artist_check_quality, time_check_quality, songplay_check_quality]
    check_quality_expected_results = [0, 0, 0, 0, 0]
