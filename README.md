Summary:
========
A music streaming startup, Sparkify want to move their data warehouse to a data lake

Source Files:
=============
This project use source as two json file which resides in AWS S3 bucket
 - Song Dataset
        Song Dataset contains a metadata about a song and artist of that songs
        
 - Log Dataset
        Log Dataset contains log details such as artist name,song title, auth mode,level,timestamps etc..

Target Tables:
==============
The source data will be transformed to five different tables which resides in S3 as partitioned parquet files

#### Dimension
**1. users**
- Basically stores details of the user such as userid, firstname, lastname, etc..

**2. songs**
- Basically stores details of the songs such as songid, title, artistid, duration of the song 
- Partitioned by year then artist

**3. artists**
- artists table contain details of the artist. it can connect with songs table using artistid

**4. time**
- time dimension is particularly developed for data analysis. Mostly users want to see their data by datetime group function
- Partitioned by year and month

#### Fact       
**songplays**
- songplays is a fact table which capture songid,artistid from log data where the record matches with song name,length and artist name
- It captures only valid record with page value equal to NextSong
- Partitioned by year and month

Python Scripts:
===============
**etl.py**
The script reads song_data and load_data from S3, transforms them to create five different tables, and writes them to partitioned parquet files in table directories on S3.

**dl.cfg**
store the s3 user credentials

Execution Procedure:
====================
###### Note: 
Ensure the AWS credentials is updated properly in dl.cfg file
1. Run the etl.py 
    ```sh
    python etl.py
    ```
**Expected output:**
In S3 bucket, Each table has its own folder within the directory. Songs table files are partitioned by year and then artist. Time table files are partitioned by year and month. Songplays table files are partitioned by year and month.  
    
            
Validation Script:
==================
validation code is added at end, after creation of parquet file (song, artist, time, user, songplays) in s3 bucket. It prints top 10 records and count of records from each file
    