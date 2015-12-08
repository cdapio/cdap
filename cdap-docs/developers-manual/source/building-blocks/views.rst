.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2015 Cask Data, Inc.

.. _views:

=====
Views
=====

*Views* are a read-only source from where data can be read. They are similar to a
:ref:`stream <streams>` or :ref:`dataset <datasets-index>`. They provide a way to
read from the same source using different schemas.

Without views, you would need to begin your data application by deciding how you wanted to
use your data, create appropriate schemas, and then fit the data to those schemas.

Now, with views, you start with your data and add schema to fit your needs. If you decide
to add a column to your schema, CDAP simply reconfigures the code that reads your data
instead of rewriting all of it. Changing the way you use your data no longer changes the
way you write it. This flexible approach is much better when you have lots of data that
you can use in many different ways, or when you are still trying to understand the data
you have.

For example:

- An admin wants to expose different parts of the same data to different stakeholders;
- A user wants to only consume a small part of a data stream for performance reasons, to
  avoid parsing entire rows of data; or
- A user has loaded in data without yet knowing or understanding how it will or can be
  used, and now wants to look at it in different ways.

Currently, views are only supported for streams. Support for datasets will be added in a
later version of CDAP.


Views and CDAP Explore
======================
:ref:`CDAP Explore <data-exploration>` needs to be :ref:`enabled <hadoop-configuration-explore-service>` 
before any streams and stream views are created so that a Hive table can be created for
each view. (See the :ref:`CDAP installation instructions <installation-index>` for your
particular Hadoop distribution for details.)

Stream View
===========
A stream view is a read-only, schema-based, view of a stream. This feature is intended to
replace the single schema associated with a stream, effectively allowing for multiple
schemas to be associated with a single stream. When a stream view is created, CDAP creates
a Hive table that reads from the associated stream and uses the schema belonging to the
stream view.

Read Formats
------------
A view has a specific read format. Read formats consist of a :ref:`format <stream-exploration-stream-format>`
(such as CSV, TSV, or Avro, among others) and a :ref:`schema <stream-exploration-stream-schema>`.

The *format* describes how the data should be parsed into higher-level objects, while the
*schema* describes how the parsed data should be interpreted (what are strings, what are
numbers, etc.) and, depending on the format, how it should be mapped between the
higher-level objects and schema columns.

Certain formats, such as CSV and TSV, use the additional *settings* attribute to describe
the mapping between the named fields and the list of higher-level objects created from the
parsing. The list is zero-based, as can be seen in this fragment of a CDAP CLI command::

  settings "mapping=0:ticker,1:num_traded,2:price"

In this example, ``0:ticker`` means map the 0th column of the CSV row to the name *ticker*, 
the 1st column to the name *num_traded*, and so on.


Stream View Lifecycle
=====================
Views can be created, deleted, listed, and their details retrieved using either:

- the :ref:`Views HTTP RESTful API <http-restful-api-views>`; or
- the :ref:`CDAP CLI <cli>`; in particular, the CLI's :ref:`Ingest Commands <cli-available-commands>`.


Creating and Modifying a Stream View
------------------------------------
A view can be added to an existing stream with either an :ref:`HTTP POST request command
<http-restful-api-view-creating-stream-view>` or a matching CDAP CLI command. In the
request body is placed a JSON object specifying the read format (:ref:`format
<stream-exploration-stream-format>` and :ref:`schema <stream-exploration-stream-schema>`)
to be used.
  
If a stream view for that stream already exists, it will be modified instead of created.
Only the response code will differ.

For example, using the CDAP CLI, this command (reformatted to fit) will create |---| for
an existing stream *trades* |---| a stream view, *view1*, with a format of *CSV* and an
appropriate schema::

  cdap > create stream-view trades view1 format csv schema "ticker string, num_traded int, price double" \
          settings "mapping=0:ticker,1:num_traded,2:price"

Listing Views and View Details
------------------------------
You can list all of the existing stream views of a stream, and see the details of each view.
For example::

  cdap > describe stream-view trades view1
  +==============================================================================================================+
  | id    | format | table             | schema            | settings                                            |
  +==============================================================================================================+
  | view1 | csv    | stream_stock_trad | {"type":"record", | {"mapping":"0:ticker,1:num_traded,2:price"}         |
  |       |        | es_view1          | "name":"rec","fie |                                                     |
  |       |        |                   | lds":[{"name":"ti |                                                     |
  |       |        |                   | cker","type":["st |                                                     |
  |       |        |                   | ring","null"]},{" |                                                     |
  |       |        |                   | name":"num_traded |                                                     |
  |       |        |                   | ","type":["int"," |                                                     |
  |       |        |                   | null"]},{"name":" |                                                     |
  |       |        |                   | price","type":["d |                                                     |
  |       |        |                   | ouble","null"]}]} |                                                     |
  +==============================================================================================================+

Further information can be found in the :ref:`Views HTTP RESTful API <http-restful-api-views>`.

Deleting a Stream View
----------------------
Deleting a stream view deletes only the Hive table that was created for the view, and not
the underlying data that you are viewing.

This example uses the CDAP CLI to delete the stream view, *view1*, created with the previous command:

  cdap > delete stream-view stock_trades view1
  Successfully deleted stream-view 'view1'


Stream View Examples
====================
Let's create some simple stream views, using the CDAP CLI, and see how the same data can
be viewed differently.

First, from within the CDAP CLI, create a stream of stock *trades*, and add a few records::

  cdap > create stream trades
  Successfully created stream with ID 'trades'
  
  cdap > send stream trades "AAPL,50,112.98"
  cdap > send stream trades "AAPL,100,112.87"
  cdap > send stream trades "AAPL,8,113.02"
  cdap > send stream trades "NFLX,10,437.45"
  Successfully sent stream event to stream 'trades'
  
Now, create a stream view, *view1*, with a format of *CSV* and an appropriate schema and mapping (reformatted to fit)::

  cdap > create stream-view trades view1 format csv schema "ticker string, num_traded int, price double" \
          settings "mapping=0:ticker,1:num_traded,2:price"  
  Successfully created stream-view 'view1'
  
Read from the stream directly, and you will receive the raw data that was sent to the stream::

  cdap > execute "select * from stream_trades"

  +=======================================================================+
  | stream_trades.ts: BIGINT | stream_trades | stream_trades.body: STRING |
  |                          | .headers: map |                            |
  |                          | <string,strin |                            |
  |                          | g>            |                            |
  +=======================================================================+
  | 1449272167321            | {}            | AAPL,50,112.98             |
  | 1449272174028            | {}            | AAPL,100,112.87            |
  | 1449272180252            | {}            | AAPL,8,113.02              |
  | 1449272186660            | {}            | NFLX,10,437.45             |
  +=======================================================================+
  Fetched 4 rows
  
Now, read from the stream view *view1*::

  cdap > execute "select * from stream_trades_view1"
  
  +==============================================================================================================+
  | stream_trades_view1 | stream_trades_view1 | stream_trades_view1 | stream_trades_view1 | stream_trades_view1. |
  | .ts: BIGINT         | .headers: map<strin | .ticker: STRING     | .num_traded: INT    | price: DOUBLE        |
  |                     | g,string>           |                     |                     |                      |
  +==============================================================================================================+
  | 1449272167321       | {}                  | AAPL                | 50                  | 112.98               |
  | 1449272174028       | {}                  | AAPL                | 100                 | 112.87               |
  | 1449272180252       | {}                  | AAPL                | 8                   | 113.02               |
  | 1449272186660       | {}                  | NFLX                | 10                  | 437.45               |
  +==============================================================================================================+
  Fetched 4 rows
  
You can treat the stream view just as you would any other explorable stream, and run SQL
queries. This query totals all the values for each stock::

  cdap > execute "select ticker, count(*) as transactions, sum(num_traded) as volume from stream_trades_view1 group by ticker order by volume desc"
  
  +========================================================+
  | ticker: STRING | transactions: BIGINT | volume: BIGINT |
  +========================================================+
  | AAPL           | 3                    | 158            |
  | NFLX           | 1                    | 10             |
  +========================================================+
  Fetched 2 rows

You can create and view an additional stream view, *view2*, with just a single column::

  cdap > create stream-view trades view2 format csv schema "num_traded int" settings "mapping=1:num_traded"
  cdap > execute "select * from stream_trades_view2"
  
  +========================================================================================================+
  | stream_trades_view2.ts: BIGINT | stream_trades_view2.he | stream_trades_view2.num_traded: INT          |
  |                                | aders: map<string,stri |                                              |
  |                                | ng>                    |                                              |
  +========================================================================================================+
  | 1449272167321                  | {}                     | 50                                           |
  | 1449272174028                  | {}                     | 100                                          |
  | 1449272180252                  | {}                     | 8                                            |
  | 1449272186660                  | {}                     | 10                                           |
  +========================================================================================================+
  Fetched 4 rows

Note that the second view only sees the columns that were defined for that view; this
allows you to separate out the data to just the entries that are desired or permissable to
be viewed.
