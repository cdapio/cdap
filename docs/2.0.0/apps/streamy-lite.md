What is Streamy Lite?
======================================

Streamy Lite is a personalized RSS recommendation system. It personalizes content at different levels: from most popular article (based on crowd activity) to deeply personalized (based on your own activity on Streamy Lite). Streamy Lite is powered by the Continuuity Reactor.

### System Overview
The Streamy Lite backend consists of four major components:
Content Management System (CMS)
Crawler
Targetting & Ranking system
Analytics
Streamy Lite CMS System
Streamy Lite CMS is a simple content management system for managing all of the news items ingested by the crawler. It enriches the content in the pipeline during ingestion and does queries to retrieve data. The enrichment pipeline is easily extendable. Currently, it supports Lucene-based standard analysis, sentiment analysis, and named entity recognition.
Continuuity Reactor Elements
Flow
Supports “news event” real-time processing pipeline with an extended enrichment pipeline.
Supports processing of editorial inputs. Example: removing an item from the system or labeling an item with more information.
Stream
NEWS content ingestion (RESTful API)
Procedures
Capability to retrieve metadata
Enrichment features generated in the pipeline.
MapReduce
CMS housekeeping (cleaning old items from the data model).
Dataset
The CMS data model is stored via datasets.
News Stream Interface
news-stream
Streams data via JSON API:
```
 [
     {
        "feedId": "url-of-feed",
        "title", "title-of-news-item",
        "description", "description-of-news-item",
        "image", "image-url-if-any",
        "url", "url-to-news-item",
        "guid", "globally-unique-identifier"
     },
     {
        ...
     }
  ]
```

editorial-stream
Streams data via JSON API:
 {
    "id" : "",
    "rule" : "",
  }

Streamy Lite Crawler System
The Streamy Lite Crawler is an RSS feed scheduler and parser. It is responsible for keeping the CMS content fresh. It keeps track of the feeds that need to be refreshed via the BigFlow capability “Tick” and do periodic checks for feeds that need to be refreshed. Feeds can be scheduled with various update frequencies and can also be deleted from the Crawler.
Reactor Elements Used by the Crawler
Flow
Processes “news feed” requests to be added or deleted, and updates the frequency of the crawl.
Scheduler keeps track of feeds that need to be updated.
Stream
Feeds URL mangement events.
Procedures
None
MapReduce
None
DataSet
MultiStoreObject<CrawlState>
Stream Interface
feed-url
Streams data via JSON API:
{
   "url" : "feed-url-to-be-acted-upon",
   "action" : "add or delete",
   "frequency" : "time in seconds"
 }

Streamy Lite Analytics
Streamy Lite Analytics provides real-time analytics. It uses the continuuity Cube dataset to compute real-time OLAP aggregations on multiple user and content dimensions.
Reactor Elements Used by Streamy Lite Analytics
Flow
Processes user events and updates the Cube dataset to perform streaming OLAP aggregations.
Stream
Ingests user events.
Procedures
Retrives aggregations across multiple user and content dimensions.
MapReduce
None
DataSet
Cube
Stream Interface
user-events
Streams data via JSON API:
 {
    "action": "user-action click or view",
    "contents": [
        {
            "id": "content-id of the content acted upon",
            "pos": position of the content in the module
        },
    ...
    ],
    "module": "name-of-the-module",
    "ts": timestamp of user action,
    "id": "id of the user",
    "ip": "user's ip address"
  }

Targeting and Ranking Systems