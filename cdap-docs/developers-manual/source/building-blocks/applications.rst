.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2014 Cask Data, Inc.

.. _applications:

============================================
Applications
============================================

.. highlight:: java

An **Application** is a collection of application virtualizations that read from—and write
to—the data virtualization layer in CDAP. Application virtualizations include *Programs,* *Services,* and *Procedures.*

Programs include :doc:`Flows <flows-flowlets/index>`, :doc:`MapReduce Jobs <mapreduce-jobs>`,
:doc:`Workflows <workflows>`, and :doc:`Spark Jobs <spark-jobs>`, and are used to process
data. :doc:`Services <services>` and :doc:`Procedures <procedures>` are used to serve data.

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
