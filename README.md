# DataEngineerChallengeAnswer

## Processing & Analytical goals:

1. Sessionize the web log by IP. Sessionize = aggregrate all page hits by visitor/IP during a session.
    https://en.wikipedia.org/wiki/Session_(web_analytics)

2. Determine the average session time

3. Determine unique URL visits per session. To clarify, count a hit to a unique URL only once per session.

4. Find the most engaged users, ie the IPs with the longest session times

## Tools Used:
- Spark (PySpark)
- Jupyter Notebook (.ipynb file included)

## Additional Library Used:
- Pandas
- Gzip
- AwsLogParser

## How to Read the Answer:
- All of the functions related to each of the answers are included in the `answer.py` script file. The `.ipynb` file is also included if you want to run it via Jupyter Notebook.
- The script has been given several comments to give more context on which function answers which question, as well as on how the logic works.
- In general, the way script works are:
   - Unarchive the log file and parse it using AwsLogParser library, convert to pandas dataframe
   - Add some additional column needed to answer the question
   - Open the Spark session, convert the pandas dataframe to spark dataframe. Schema defined manually based on the AWS ELB log format
   - Do some preprocessing by adding some more columns needed
   - **Answer no.1**: Adding session id column per client ip with the 15 mins (900 secs) window of inactivity.
   - **Answer no.2**: Aggregating all total time per session id
   - **Answer no.3**: Count unique URLs hit per session
   - **Answer no.4**: Count total time per client ip from all session
- There are `sample_log` files in data directory which I used as a sample to do the analysis (contains 200 out of total of 20k rows) in order to reduce the resource needed. Change the `sample_log` lines in the script to the original file name if you want to run it using the original data.
