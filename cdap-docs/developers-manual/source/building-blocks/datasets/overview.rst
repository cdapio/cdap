.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2014-2016 Cask Data, Inc.

.. _datasets-overview:

=================
Datasets Overview
=================

Introduction
============

.. highlight:: java

The core datasets of CDAP are *Tables* and *FileSets*:

- Unlike relational database systems, CDAP *Tables* are not organized into rows with a fixed schema.
  They are optimized for efficient storage of semi-structured data, data with unknown or variable
  schema, or sparse data.
  
- CDAP *FileSets* provide an abstraction over the raw file system, and associate properties such as
  the format or the schema with the files they contain. In addition, partitioned file sets
  allow addressing files by their partition meta data, removing the need for applications to
  be aware of actual file system locations.

Other datasets are built on top of tables and file sets. A dataset can implement
specific semantics around a core dataset, such as a key/value Table or a
counter Table. A dataset can also combine multiple datasets to create a
complex data pattern. For example, an indexed Table can be implemented
by using one Table for the data and a second Table for the index of that data.

A number of useful datasets |---| we refer to them as system datasets |---| are
included with CDAP, including key/value tables, indexed tables and
time series. You can implement your own data patterns as custom
datasets, on top of any combination of core and system datasets.

Creating a Dataset
==================

You can create a dataset in CDAP using either the :ref:`http-restful-api-dataset`, the
:ref:`Command Line Interface<cli>`, or in an :ref:`application specification. <applications>`

You tell applications to create a dataset if it does not already
exist by declaring the dataset details in the application specification.
For example, to create a DataSet named *myCounters* of type 
`KeyValueTable <../../../reference-manual/javadocs/co/cask/cdap/api/dataset/lib/KeyValueTable.html>`__, write::

  public void configure() {
      createDataset("myCounters", KeyValueTable.class);
      ...

Names (*myCounters*) that start with an underscore (``_``) will not be visible in the home
page of the :ref:`CDAP UI <cdap-ui>`, though they will be visible elsewhere in the CDAP UI.

.. _datasets-in-programs:

Using Datasets in Programs
==========================

There are two ways to use a dataset in a program:

.. _static-dataset-instantiation:

Static Instantiation
--------------------

You can instruct the CDAP runtime system to inject the dataset into a class member
with the ``@UseDataSet`` annotation::

  class MyFlowlet extends AbstractFlowlet {
    @UseDataSet("myCounters")
    private KeyValueTable counters;
    ...
    void process(String key) {
      counters.increment(key.getBytes(), 1L);
    }


When starting the program, the runtime system reads the dataset specification from the metadata store and injects
an instance of the dataset class into the application. This dataset will participate in every transaction that is
executed by the program. If the program is multi-threaded (for example, an HTTP service handler), CDAP will make
sure that every thread has its own instance of the dataset.

The ``@UseDataSet`` annotation is evaluated when your program is compiled. If you don't know the name of the
dataset at compile time |---| perhaps because it is given by the application configuration |---| you can still use the
``useDatasets`` method in the ``configure`` method of your program. Note that this will then require knowledge of
the dataset at the time that the application is deployed, as that is when the ``configure`` method is executed::

  class MyFlowlet extends AbstractFlowlet {

    @Override
    public void configure(FlowletConfigurer configurer) {
      super.configure(configurer);
      useDatasets("myCounters");
    }

    void process(String key) {
      KeyValueTable counters = getContext().getDataset("myCounters");
      counters.increment(key.getBytes(), 1L);
    }

The ``useDatasets()`` call has the effect that the dataset is instantiated when the program
starts up, and it remains in the cache for the lifetime of the program and hence never needs
to be instantiated again. Note that you still need to call ``getDataset()`` every time you
access it.

.. _dynamic-dataset-instantiation:

Dynamic Instantiation
---------------------
If you don't know the name of the dataset at deploy time (and hence you cannot use
static instantiation), or if you want to use a dataset only for a short time, you can dynamically
request an instance of the dataset through the program context::

  class MyFlowlet extends AbstractFlowlet {
    ...
    void process(String key) {
      KeyValueTable counters = getContext().getDataset("myCounters");
      counters.increment(key.getBytes(), 1L);
    }

This dataset is instantiated at runtime, in this case every time the method ``process`` is invoked. To reduce the
overhead of repeatedly instantiating the same dataset, the CDAP runtime system caches dynamic datasets internally.
By default, the cached instance of a dataset will not expire, and it will participate in all transactions initiated
by the program.

For convenience, if you know the dataset name at the time the program starts, you can store a reference to
the dataset in a member variable at that time (similar to static datasets, but assigned explicitly by you)::

  class MyFlowlet extends AbstractFlowlet {

    private KeyValueTable counters;

    @Override
    public void initialize(FlowletContext context) throws Exception {
      super.initialize(context);
      counters = context.getDataset("myCounters");
    }

    void process(String key) {
      counters.increment(key.getBytes(), 1L);
    }

See :ref:`Word Count <examples-word-count>` for an example of how this can be used to configure
the dataset names used by an application.

Contrary to static datasets, dynamic datasets allow the release of the resources held by their Java classes
after you are finished using them. You can do that by calling the ``discardDataset()`` method of the program context:
it marks the dataset to be closed and removed from all transactions. However, this will not happen until after the
current transaction is complete, because the discarded dataset may have performed data operations and therefore
still needs to participate in the commit (or rollback) of the transaction.
  
Discarding a dataset is useful:

- To ensure that the dataset is closed and its resources are released, as soon as a program does not need the dataset
  any longer.
- To refresh a dataset after its properties have been updated. Without discarding the dataset, the program would keep
  the dataset in its cache and never pick up a change in the dataset's properties. Discarding the dataset
  ensures that it is removed from the cache after the current transaction. Therefore, the next time this dataset is
  obtained using ``getDataset()`` in a subsequent transaction, it is guaranteed to return a fresh instance of the
  dataset, hence picking up any properties that have changed since the program started.

It is important to know that after discarding a dataset, it remains in the cache for the duration of the current
transaction. Be aware that if you call ``getDataset()`` again for the same dataset and arguments before the
transaction is complete, then that reverses the effect of discarding. It is therefore a good practice to discard
a dataset at the end of a transaction.

Similarly to static datasets, if a program is multi-threaded, CDAP will make
sure that every thread has its own instance of each dynamic dataset |---| and in order to discard a dataset
from the cache, every thread that uses it must individually call ``discardDataset()``.

.. _cross-namespace-dataset-access:

Cross-namespace Dataset Access
------------------------------
The dataset usage methods described above allow accessing datasets from the same :ref:`namespace <namespaces>` in
which the program exists. However, :ref:`Dynamic Dataset Instantiation <dynamic-dataset-instantiation>` also allows
users to access datasets from a different namespace than the one in which the program accessing the dataset is running.
Typically, this may be required in scenarios where datasets are large enough to warrant sharing across namespaces, as
opposed to every namespace having its own copy. To use a dataset from a different namespace, users can pass a
``namespace`` parameter to ``getDataset()``::

  class MyFlowlet extends AbstractFlowlet {

    private KeyValueTable counters;

    @Override
    public void initialize(FlowletContext context) throws Exception {
      super.initialize(context);
      counters = context.getDataset("otherNamespace", "myCounters");
    }

    void process(String key) {
      counters.get(key.getBytes());
    }

Using this API, users can both read and write to a dataset in a different namespace.

Cross namespace access is not supported using :ref:`Static Dataset Instantiation <static-dataset-instantiation>`, since
doing so would require users to know the namespace at the time of development of the application.

**Note**: On clusters with :ref:`authorization <admin-authorization>` enabled, please refer to
:ref:`authorization policy pushdown <security-auth-policy-pushdown>` for additional instructions.

.. _dataset-admin-in-programs:

Dataset Management in Programs
==============================

Instantiating a dataset in a program allows you to perform any of the dataset's data operations |---| the Java
methods defined in the dataset's API. However, you cannot perform administrative operations such as creating or
dropping a dataset. For these operations, the program context offers an ``Admin`` interface that can be obtained
through the ``getAdmin()`` method of the context. This is available in all types of programs. For example, in
a service handler, you can obtain the ``Admin`` through the ``HttpServiceContext``. The ``FileSetHandler`` of the
:ref:`examples-fileset` extends ``AbstractHttpServiceHandler`` |---| its ``configure`` method saves the context
in an instance variable and makes it available through ``getContext()``:

.. literalinclude:: /../../../cdap-examples/FileSetExample/src/main/java/co/cask/cdap/examples/fileset/FileSetService.java
    :language: java
    :lines: 63-68
    :dedent: 2

The handler defines several endpoints for dataset management, one of which can be used to create a new file set,
either by cloning an existing file set's dataset properties, or by using the properties submitted in the request body:

.. literalinclude:: /../../../cdap-examples/FileSetExample/src/main/java/co/cask/cdap/examples/fileset/FileSetService.java
    :language: java
    :lines: 167-196
    :dedent: 2

For more details, see the :ref:`examples-fileset`.

Note that even though you can call dataset management methods within a transaction, these operations are *not*
transactional, and they are not rolled back in case the current transaction fails. It is advisable not to mix data
operations and dataset management operations within the same transaction.

Dataset Time-To-Live (TTL)
==========================

Datasets, like :ref:`streams <streams>`, can have a Time-To-Live (TTL) property that
governs how long data will be persisted in a specific dataset. TTL is configured as the
maximum age (in seconds) that data should be retained.

When you create a dataset, you can configure its TTL as part of the creation::

  public void configure() {
      createDataset("myCounters", Table.class, 
                    DatasetProperties.builder().add(Table.PROPERTY_TTL, 
                                                    "<age in seconds>").build());
      ...
  }

The default TTL for all datasets is infinite, meaning that data will never expire. The TTL
property of an existing dataset can be changed using the :ref:`http-restful-api-dataset`.

Types of Datasets
=================

A dataset abstraction is defined by a Java class that implements the ``DatasetDefinition`` interface.
The implementation of a dataset typically relies on one or more underlying (embedded) datasets.
For example, the ``IndexedTable`` dataset can be implemented by two underlying Table datasets |---|
one holding the data and one holding the index.

We distinguish three categories of datasets: *core*, *system*, and *custom* datasets:

- The |core|_ datasets of the CDAP are Table and FileSet. Their implementations may use internal
  CDAP classes hidden from developers.

- A |system|_ dataset is bundled with the CDAP and is built around
  one or more underlying core or system datasets to implement a specific data pattern.

- A |custom|_ dataset is implemented by you and can have arbitrary code and methods.
  It is typically built around one or more Tables, FileSets (or other datasets)
  to implement a specific data pattern.

Each dataset is associated with exactly one dataset implementation to
manipulate it. Every dataset has a unique name and metadata that defines its behavior.
For example, every ``IndexedTable`` has a name and indexes a particular column of its primary table:
the name of that column is a metadata property of each dataset of this type.

.. |core| replace:: **core**
.. _core: index.html#core-datasets

.. |system| replace:: **system**
.. _system: system-custom.html#system-datasets

.. |custom| replace:: **custom**
.. _custom: system-custom.html#custom-datasets


.. _core-datasets:

Core Datasets
-------------

**Tables** and **FileSets** are the core datasets,
and all other datasets are built using combinations of Tables and FileSets.

While these Tables have rows and columns similar to relational database tables, there are key differences:

- Tables have no fixed schema. Unlike relational database tables where every
  row has the same schema, every row of a Table can have a different set of columns.

- Because the set of columns is not known ahead of time, the columns of
  a row do not have a rich type. All column values are byte arrays and
  it is up to the application to convert them to and from rich types.
  The column names and the row key are also byte arrays.

- When reading from a Table, one need not know the names of the columns:
  The read operation returns a map from column name to column value.
  It is, however, possible to specify exactly which columns to read.

- Tables are organized in a way that the columns of a row can be read
  and written independently of other columns, and columns are ordered
  in byte-lexicographic order. They are also known as *Ordered Columnar Tables*.

A |fileset|_ represents a collection of files in the file system that share some common attributes
such as the format and schema, while abstracting from the actual underlying file system interfaces.

.. |fileset| replace:: **FileSet**
.. _fileset: fileset.html

- Every file in a FileSet is in a location relative to the FileSet's base directory.

- Knowing a file's relative path, any program can obtain a ``Location`` for that file through a method
  of the FileSet dataset. It can then interact directly with the file's Location; for example, to write
  data to the Location, or to read data from it.

- A FileSet can be used as the input or output to MapReduce. The MapReduce program need not specify
  the input and output format to use, or configuration for these |---| the FileSet dataset provides this
  information to the MapReduce runtime system.

- An abstraction of FileSets, ``PartitionedFileSets`` allow the associating of meta data (partitioning keys)
  with each file. The file can then be addressed through its meta data, removing the need for programs to
  be aware of actual file paths.
  
- A ``TimePartitionedFileSet`` is a further variation that uses a timestamp as the partitioning key.
  Though it is not required that data in each partition be organized by time,
  each partition is assigned a logical time. 

  This is in contrast to a :ref:`Timeseries Table <cdap-timeseries-guide>` dataset, where
  time is the primary means of how data is organized, and both the data model and the
  schema that represents the data are optimized for querying and aggregating over time
  ranges.

  Time-partitioned FileSets are typically written in batch: into large files, every *N* minutes or
  hours...while a timeseries table is typically written in real-time, one data point at a
  time.
 
- A ``CubeDataset`` is a multidimensional dataset, optimized for data warehousing and OLAP
  (Online Analytical Processing) applications. A Cube dataset stores multidimensional facts,
  provides a querying interface for retrieval of data and allows exploring of the stored data.

  See :ref:`Cube Dataset <datasets-cube>` for: details on configuring a Cube dataset;
  writing to and reading from it; and querying and exploring the data in a cube.
  
  An example of using the Cube dataset is provided in the :ref:`Data Analysis with OLAP
  Cube <cdap-cube-guide>` guide. 
  

Dataset Examples
================
Datasets are included in just about every CDAP :ref:`application <apps-and-packs>`,
:ref:`tutorial <tutorials>`, :ref:`guide <guides-index>` or :ref:`example <examples-index>`.

- The simplest example, :ref:`Hello World <examples-hello-world>`, demonstrates using a
  stream to **ingest a name into a dataset.**

- For examples of **custom datasets,** see the :ref:`Word Count <examples-word-count>`
  and :ref:`Web Analytics <examples-web-analytics>` examples.

- For an example of **a dataset and a Map Reduce Job,** see the :ref:`Purchase
  <examples-purchase>` example.

- For an example of a **Table dataset,** see the :ref:`Word Count <examples-word-count>` example.

- For an example of a **Cube dataset,** see the :ref:`Data Analysis with OLAP Cube <cdap-cube-guide>` guide.

- For an example of a **FileSet dataset,** see the :ref:`FileSet <examples-fileset>` example.

- For an example of a **PartitionedFileSet,** see the :ref:`Sport Results <examples-sport-results>`
  example.

- For an example of a **TimePartitionedFileSet,** see the :ref:`StreamConversion 
  <examples-stream-conversion>` example.

- For examples of **key-value Table datasets,** see the
  :ref:`Hello World <examples-hello-world>`,
  :ref:`Count Random <examples-count-random>`,
  :ref:`Word Count <examples-word-count>`, and
  :ref:`Purchase <examples-purchase>` examples.

- For an example of an **ObjectMappedTable dataset,** see the :ref:`Purchase <examples-purchase>` example.

- For examples of **ObjectStore datasets,** see the :ref:`Purchase <examples-purchase>`,
  :ref:`Spark K-Means <examples-spark-k-means>`, and :ref:`Spark Page Rank <examples-spark-page-rank>` examples.

- For an example of a **Timeseries Table dataset,** see the how-to guide :ref:`cdap-timeseries-guide`.
