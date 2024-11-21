
# Raw robots file, 10.2021 - 10.2024
Kyle Matoba, 01.11.2024

This dataset contains the `robots.txt` files from all commoncrawl `response`s between October 2021 and October 2024: 

Here are the constituent crawls, with the number of 200-like responses and the time period covered.
```
crawl name       # row    first datetime         last datetime
CC-MAIN-2021-43  88150767 2021-10-15T19:25:17Z - 2021-10-28T22:35:51Z
CC-MAIN-2021-49  86901347 2021-11-26T22:41:26Z - 2021-12-09T15:24:47Z

CC-MAIN-2022-05  81774170 2022-01-16T09:32:03Z - 2022-01-29T15:17:10Z
CC-MAIN-2022-21  88321885 2022-05-16T04:14:19Z - 2022-05-29T13:38:39Z
CC-MAIN-2022-27  86843004 2022-06-24T21:39:49Z - 2022-07-07T18:40:45Z
CC-MAIN-2022-33  89962948 2022-08-07T15:09:49Z - 2022-08-20T07:24:07Z
CC-MAIN-2022-40  82487425 2022-09-24T15:16:04Z - 2022-10-08T00:04:49Z
CC-MAIN-2022-49  87284951 2022-11-26T08:07:55Z - 2022-12-10T10:42:38Z

CC-MAIN-2023-06  78733681 2023-01-26T21:09:11Z - 2023-02-09T14:25:02Z
CC-MAIN-2023-14  81150267 2023-03-20T08:35:35Z - 2023-04-02T13:50:18Z
CC-MAIN-2023-23  85454018 2023-05-27T22:35:39Z - 2023-06-11T02:30:16Z
CC-MAIN-2023-40  90317330 2023-09-21T07:37:33Z - 2023-10-05T04:20:01Z
CC-MAIN-2023-50 101028224 2023-11-28T08:35:09Z - 2023-12-12T00:04:04Z

CC-MAIN-2024-10  94733131 2024-02-20T21:11:18Z - 2024-03-05T15:40:46Z
CC-MAIN-2024-18  92143840 2024-04-12T10:14:20Z - 2024-04-25T16:02:22Z
CC-MAIN-2024-22  97644816 2024-05-17T23:31:46Z - 2024-05-31T00:00:41Z
CC-MAIN-2024-26  99715321 2024-06-12T14:04:48Z - 2024-06-25T23:27:59Z
CC-MAIN-2024-30  94443660 2024-07-12T09:42:39Z - 2024-07-25T20:50:31Z
CC-MAIN-2024-33  90738661 2024-08-02T23:45:30Z - 2024-08-16T05:58:25Z
CC-MAIN-2024-38  91196581 2024-09-07T09:59:21Z - 2024-09-21T04:50:53Z
CC-MAIN-2024-42  90254842 2024-10-03T09:40:44Z - 2024-10-16T09:06:42Z
```

It consists of 1879108073 rows, about 1.3TB in the format below. 

Below I document the steps to recreate it. 

# Download commoncrawl dumps

 - Get the `aws` command, via `pip install awscli`.
 - Then run some variant of `aws s3 ls s3://commoncrawl/crawl-data/CC-MAIN-2024-42/segments/ --recursive |  awk '{print $4}' | grep 'robotstxt/.*\.warc\.gz$' | xargs -P 100 -I {} aws s3 cp s3://commoncrawl/{} .`
 - It is free to access, cf. `https://commoncrawl.org/get-started`. Note that the number of parallel downloaders (`-P` argument above), should not be too high, otherwise you risk being throttled by AWS. I found that I was never rate limited with 100, and that I was sometimes with 100, but ymmv.
 - Each crawl contains about 90,000 warc files lately, each of which is around 2mb.
 - Note that in principle it is possible to adapt the pipeline to read directly from S3, see the bottom of 
https://resiliparse.chatnoir.eu/en/stable/man/fastwarc.html. We didn't know this beforehard, but if starting from scratch it's probably better. 


# Build a database per dump
  It would be nice to build giant key-value store from the union of all folders in a parallelized fashion.
  However, concurrent writes to sqlite do not work well. 
  Thus, we construct a distinct databases for each crawl as an intermediate step. 
  This is done with the `build_db.py` script.
  It features a simple resumption mechanism in case, of interruption, in the `metadata` subfolder.  

Each database contains a single table `robots`, which has five columns:
 - `url`: string, e.g. `http://0v.tittrtb.cn/robots.txt`.
 - `timestamp`: string (should be int), seconds from epoch in utc, you can convert a timestamp `t` to a tz aware datetime with `recovered = dt.datetime.fromtimestamp(t, dt.UTC)` for `dt = datetime`.
 - `status_code`: string: 
 - `response`: string (bytes), the content of the webpage `b'User-agent: * \r\nDisallow: /plus/ad_js.php\r\nDisallow: /plus/advancedsearch.php\r\nDisallow: /plus/car.php\r\nDisallow: /plus/carbuyaction.php\r\nDisallow: /plus/shops_buyaction.php\r\nDisallow: /plus/erraddsave.php\r\nDisallow: /plus/posttocar.php\r\nDisallow: /plus/disdls.php\r\nDisallow: /plus/feedback_js.php\r\nDisallow: /plus/mytag_js.php\r\nDisallow: /plus/rss.php\r\nDisallow: /plus/search.php\r\nDisallow: /plus/recommend.php\r\nDisallow: /plus/stow.php\r\nDisallow: /plus/count.php\r\nDisallow: /include\r\nDisallow: /templets'`
 - `provenance`: string, the name of the warc file from which the  . E.g. `CC-MAIN-20211015192439-20211015222439-00005`.

We keep only 200-like responses.

There is no key encoded into the database, but logically it is keyed by `(url, timestamp)`.

# Concatenate databases

Next, concatenate the databases together. I found this to take 7-8 minutes per insertion. It could be sped up by concatenating pairs in parallel. 

# Fix large database
We are interested in the last `robots.txt` by url. So we run a query on the resultant database to keep only the entry with the latest date. The exact SQL call is in `build_latest_robots.py`, and in particular creates a new table called `latest_robots`. It contains around 250,000,000 rows. 

# Use database to build the last entry per `url`:

To do in next run (already implemented, just need to run)
 - Local, not absolute path in provenance
 - integer Timestamp, integer return code
 - 
