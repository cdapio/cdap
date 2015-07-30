.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2015 Cask Data, Inc.

===============================
Sources: Real-time: Twitter 
===============================

.. rubric:: Description

Samples tweets in real-time. Output records will have this schema::

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

.. rubric:: Use Case

The source is used whenever you want to sample tweets from Twitter in real-time.
For example, you may want to read tweets and store them in a table where they can
be accessed by your data scientists to perform experiments.

.. rubric:: Properties

See the `Twitter OAuth documentation <https://dev.twitter.com/oauth/overview>`__
for more information on obtaining your access token and access token secret.
The consumer key and secret are specific to your Twitter app. Login, view
`your apps <https://apps.twitter.com/>`__, then click on the relevant app to find the
consumer key and secret.

**ConsumerKey:** Consumer Key

**ConsumerSecret:** Consumer Secret

**AccessToken:** Access Token

**AccessTokenSecret:** Access Token Secret

.. rubric:: Example

::

  {
    "name": "Twitter",
    "properties": {
      "AccessToken": "GetAccessTokenFromTwitter",
      "AccessTokenSecret": "GetAccessTokenSecretFromTwitter",
      "ConsumerKey": "GetConsumerKeyFromTwitter",
      "ConsumerSecret": "GetConsumerSecretFromTwitter"
    }
  }

