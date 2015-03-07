.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2014-2015 Cask Data, Inc.

============================================
Core Abstractions
============================================

.. rubric:: Data Abstractions

There are two main data abstractions: *Streams* and *Datasets*. Streams are ordered,
partitioned sequences of data, and are the primary means of bringing data from external
systems into the CDAP in realtime. Datasets are abstractions on top of data, allowing you
to access your data using higher-level abstractions and generic, reusable Java
implementations of common data patterns instead of requiring you to manipulate data with
low-level APIs.

.. rubric:: Application Abstraction

Applications hide low-level details of individual programming paradigms and runtimes,
while providing access to many useful and powerful services provided by CDAP such as 
distributed transactions, service discovery, and the ability to dynamically scale
processing units.

Applications are abstracted away from the platform that runs the application. When you
deploy and run the application into a specific installation of CDAP, the appropriate
implementations of all services and program runtimes are injected by CDAP; the application
does not need to change based on the environment. This allows you develop applications in
one environment—such as on your laptop using a standalone CDAP for testing—and then
seamlessly deploy it in a different environment—such as your distributed staging cluster.

.. rubric:: Data and Applications Combined

With your data represented in CDAP as *Streams* and *Datasets*, you are able to process
that data in realtime or in batch using a Program (*Flow,* *MapReduce*, *Spark*,
*Workflow*) and you can serve data to external clients using a *Service*.

This diagram how the CDAP components relate in an Apache Hadoop installation: 

.. image:: ../_images/architecture_diag.png
   :width: 7in
   :align: center
