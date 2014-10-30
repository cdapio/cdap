.. :author: Cask Data, Inc.
   :description: placeholder
   :copyright: Copyright © 2014 Cask Data, Inc.

.. _applications:

============================================
Application
============================================

.. highlight:: java

An **Application** is a collection of Programs, Services, and Procedures that read from
and write to the data virtualization layer in CDAP. Programs include `Flows,
<flows/index>`__ `MapReduce Jobs, <mapreduce-jobs>`__ `Workflows, <workflows>`__ and
`Spark Jobs, <spark-jobs>`_ and are used to process data. Services and Procedures are used
to serve data.

In writing a CDAP Application, it's best to use an integrated development environment that
understands the application interface to provide code-completion in writing interface
methods.

To create an Application, implement the ``Application`` interface or subclass from
``AbstractApplication`` class, specifying the Application metadata and declaring and
configuring each of the Application elements::

      public class MyApp extends AbstractApplication {
        @Override
        public void configure() {
          setName("myApp");
          setDescription("My Sample Application");
          addStream(new Stream("myAppStream"));
          addFlow(new MyAppFlow());
          addProcedure(new MyAppQuery());
          addMapReduce(new MyMapReduceJob());
          addWorkflow(new MyAppWorkflow());
        }
      }

Notice that *Streams* are defined using the provided ``Stream`` class, and are referenced by
names, while other components are defined using user-written classes that implement
correspondent interfaces and are referenced by passing an object, in addition to being
assigned a unique name.

Names used for *Streams* and *Datasets* need to be unique across the CDAP instance, while
names used for Programs and Services need to be unique only to the application.

