# Twitter Streaming


Description
-----------
Samples tweets in real-time through Spark streaming. Output records will have this schema:

    +================================+
    | field name  | type             |
    +================================+
    | id          | long             |
    | message     | string           |
    | lang        | nullable string  |
    | time        | nullable long    |
    | favCount    | int              |
    | rtCount     | int              |
    | source      | nullable string  |
    | geoLat      | nullable double  |
    | geoLong     | nullable double  |
    | isRetweet   | boolean          |
    +================================+


Use Case
--------
The source is used whenever you want to sample tweets from Twitter in real-time using Spark streaming.
For example, you may want to read tweets and store them in a table where they can
be accessed by your data scientists to perform experiments.


Properties
----------
**referenceName:** This will be used to uniquely identify this source for lineage, annotating metadata, etc.

See the [Twitter OAuth documentation] for more information on obtaining
your access token and access token secret. The consumer key and secret
are specific to your Twitter app. Login, view [your apps], then click on
the relevant app to find the consumer key and secret.

  [Twitter OAuth documentation]: https://dev.twitter.com/oauth/overview
  [your apps]: https://apps.twitter.com/

**ConsumerKey:** Twitter Consumer Key.

**ConsumerSecret:** Twitter Consumer Secret.

**AccessToken:** Twitter Access Token.

**AccessTokenSecret:** Twitter Access Token Secret.


Example
-------

    {
        "name": "Twitter",
        "type": "streamingsource",
        "properties": {
            "AccessToken": "GetAccessTokenFromTwitter",
            "AccessTokenSecret": "GetAccessTokenSecretFromTwitter",
            "ConsumerKey": "GetConsumerKeyFromTwitter",
            "ConsumerSecret": "GetConsumerSecretFromTwitter"
        }
    }

---
- CDAP Pipelines Plugin Type: streamingsource
- CDAP Pipelines Version: 1.7.0
