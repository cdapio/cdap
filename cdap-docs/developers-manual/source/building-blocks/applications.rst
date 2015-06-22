.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2014-2015 Cask Data, Inc.

.. _applications:

============
Applications
============

.. highlight:: java

An *Application* is a collection of building blocks that read and write data through the data
abstraction layer in CDAP. 

*Applications* are composed from *Programs,* *Services,* and *Schedules*.

Programs include :doc:`Flows <flows-flowlets/index>`, :doc:`MapReduce programs <mapreduce-programs>`,
:doc:`Workflows <workflows>`, :doc:`Spark programs <spark-programs>`, and :doc:`Workers <workers>` are used to process
data. :doc:`Services <services>` are used to serve data.

**Data abstractions** include :doc:`Streams <streams>` and :doc:`Datasets <datasets/index>`.

.. rubric:: Creating an Application with an Application Specification

To create an application, implement the ``Application`` interface or subclass from
``AbstractApplication`` class, specifying the application metadata and declaring and
configuring each of the application components::

      public class MyApp extends AbstractApplication {
        @Override
        public void configure() {
          setName("myApp");
          setDescription("My Sample Application");
          addStream(new Stream("myAppStream"));
          createDataset("myAppDataset", Table.class);
          addFlow(new MyAppFlow());
          addService(new MyService());
          addMapReduce(new MyMapReduce());
          addWorkflow(new MyAppWorkflow());
        }
      }

Notice that *Streams* are defined using the provided ``Stream`` class, and *Datasets* are
defined by passing a ``Table`` class; both are referenced by name.

Other components are defined using user-written classes that implement correspondent
interfaces and are referenced by passing an object, in addition to being assigned a unique
name.

Names used for streams and datasets need to be unique across the CDAP instance, while
names used for programs and services need to be unique only to the application.

.. rubric:: A Typical CDAP Application

A typical design of a CDAP application consists of:

- *Streams* to ingest data into CDAP;
- *Flows*, consisting of *Flowlets* linked together, to process the ingested data
  in real time or batch;
- *MapReduce programs*, *Spark programs*, and *Workflows* for batch processing tasks;
- *Workers* for processing data in an ad-hoc manner that doesn't fit into real-time or batch paradigms
- *Datasets* for storage of data, either raw or the processed results; and
- *Services* for serving data and processed results.

Of course, not all components are required: it depends on the application. A minimal
application could include a stream, a flow, a flowlet, and a dataset. It's possible a
stream is not needed, if other methods of bringing in data are used. In the next pages,
we'll look at these components, and their interactions.
