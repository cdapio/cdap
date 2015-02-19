.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2014 Cask Data, Inc.

============================================
Anatomy of a Big Data Application
============================================

As an application developer building a Big Data application, you are primarily concerned with five areas:

- **Data Collection:** A method of getting data into the system so that it can be processed. 
  We distinguish these types of data collecting:

  - A system or application service may poll an external source for available data and then retrieve it ("pull"),
    or external clients may send data to a public endpoint of the platform ("push").
  - Data can come steadily, one event at a time ("realtime"), or many events at once in bulk ("batch").
  - Data can be acquired with a fixed schedule ("periodic"), or whenever new data is available ("on-demand").

  CDAP provides streams as a means to push events into the platform in real-time. It also provides tools that
  pull data in batch, be it periodic or on-demand, from external sources.

  Streams are a special type of datasets that are exposed as a push endpoint for external clients. They support
  ingesting events in realtime at massive scale. Events in the stream can then be consumed by applications in
  real-time or batch.

- **Data Exploration:** One of the most powerful paradigms of Big Data is the ability to
  collect and store data without knowing details about its structure. These details are only
  needed at processing time. An important step—between collecting the data and processing
  it—is exploration; that is, examining data with ad-hoc queries to learn about its
  structure and nature.

- **Data Processing:** After data is collected, we need to process it in various ways. For example:

  - Raw events are filtered and transformed into a canonical form, to ensure quality of input data for
    down-stream processing.
  - Events (or certain dimensions of the events) are counted or aggregated.
  - Events are annotated and used by an iterative algorithm to train a machine-learned model.
  - Events from different sources are joined to find associations, correlations or other views across
    multiple sources.
  - . . .

  Processing can happen in realtime, where a stream processor consumes events immediately after they are collected.
  Realtime processing provides less expressive power than other processing paradigms, but it provides insights into the
  data in a very timely manner. CDAP offers Flows as the realtime processing framework.

  Processing can also happen in batch, where many events are processed at the same time to analyze an entire data
  corpus at once. Batch processing is more powerful than realtime processing, but due its very nature is always
  time-lagging and thus often performed over historical data. In CDAP, batch processing can be done via
  Map/Reduce or Spark, and it can also be scheduled on a periodic basis as part of a workflow.

- **Data Storage:** The results of processing data must be stored in a persistent and durable way that allows other
  programs or applications to further process or analyze the data. In CDAP, data is stored in datasets.

- **Data Serving:** The ultimate purpose of processing data is not to store the results, but to make these results
  available to other applications. For example, a web analytics application may find ways to optimize the traffic
  on a website. However, these insights are worthless without a way to feed them back to the actual web application.
  CDAP allows serving datasets to external clients through procedures and services.

A CDAP application consists of combinations of these components:

- :ref:`Streams <streams>` for real-time data collection;
- Programs—:ref:`Flows, <flows-flowlets-index>` :ref:`MapReduce, <mapreduce>`
  :ref:`Spark <spark>`—for data processing in realtime or in batch;
- :ref:`Datasets <datasets-index>` for abstracted data storage; and
- :ref:`Procedures <procedures>` and :ref:`Services <user-services>`
  for data serving to external clients.

This diagram illustrates a typical Big Data application:

.. image:: ../_images/app_unified_batch_realtime.png
   :width: 8in
   :align: center

It illustrates the power of data abstraction in CDAP: a stream is not just a means to collect data; it can
be consumed by realtime and batch processing at the same time. Similarly, datasets allow sharing of data between
programs of different paradigms, realtime or batch, without compromising the consistency of the data,
because all data access happens under ACID (Atomicity, Consistency, Isolation, and Durability) guarantees.
