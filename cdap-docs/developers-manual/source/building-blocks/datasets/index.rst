.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2014 Cask Data, Inc.

.. _datasets-index:

============================================
Datasets
============================================

.. toctree::
   :maxdepth: 1
   
    Table API <table>
    File Sets <fileset>
    System and Custom Datasets <system-custom>
    Datasets and MapReduce <datasets-mapreduce>


.. rubric:: Introduction

.. highlight:: java

**Datasets** store and retrieve data. Datasets are your means of reading
from and writing to the CDAP’s storage capabilities. Instead of
forcing you to manipulate data with low-level APIs, Datasets provide
higher-level abstractions and generic, reusable implementations of
common data patterns.

The core Dataset of the CDAP is a Table. Unlike relational database
systems, these tables are not organized into rows with a fixed schema.
They are optimized for efficient storage of semi-structured data, data
with unknown or variable schema, or sparse data.

Other Datasets are built on top of Tables. A Dataset can implement
specific semantics around a Table, such as a key/value Table or a
counter Table. A Dataset can also combine multiple Datasets to create a
complex data pattern. For example, an indexed Table can be implemented
by using one Table for the data and a second Table for the index of that data.

A number of useful Datasets—we refer to them as system Datasets—are
included with CDAP, including key/value tables, indexed tables and
time series. You can implement your own data patterns as custom
Datasets on top of Tables.

.. rubric:: Creating a Dataset

You can create a Dataset in CDAP using either the :ref:`http-restful-api-dataset`, the
:ref:`Command Line Interface<cli>`, or in an :ref:`Application specification. <applications>`

You tell Applications to create a Dataset if it does not already
exist by declaring the Dataset details in the Application specification.
For example, to create a DataSet named *myCounters* of type 
`KeyValueTable <../../../reference-manual/javadocs/co/cask/cdap/api/dataset/lib/KeyValueTable.html>`__, write::

  public void configure() {
      createDataset("myCounters", KeyValueTable.class);
      ...

To use the Dataset in a Program, instruct the runtime
system to inject an instance of the Dataset with the ``@UseDataSet``
annotation::

  class MyFlowlet extends AbstractFlowlet {
    @UseDataSet("myCounters")
    private KeyValueTable counters;
    ...
    void process(String key) {
      counters.increment(key.getBytes(), 1L);
    }

The runtime system reads the Dataset specification for the key/value
table *myCounters* from the metadata store and injects an
instance of the Dataset class into the Application.

You can also implement custom Datasets by implementing the ``Dataset``
interface or by extending existing Dataset types. See the
:ref:`Purchase Example<examples-purchase>` for an implementation of a Custom Dataset.
For more details, refer to :ref:`Custom Datasets. <custom-datasets>`

.. rubric:: Types of Datasets

A Dataset abstraction is defined by a Java class that implements the ``DatasetDefinition`` interface.
The implementation of a Dataset typically relies on one or more underlying (embedded) Datasets.
For example, the ``IndexedTable`` Dataset can be implemented by two underlying Table Datasets –
one holding the data and one holding the index.

We distinguish three categories of Datasets: *core*, *system*, and *custom* Datasets:

- The |core|_ Dataset of the CDAP is a Table. Its implementation may use internal
  CDAP classes hidden from developers.

  **Note**: The latest version of CDAP added support for a new core Dataset that represents
  sets of files. See *FileSets* below for details.

- A |system|_ Dataset is bundled with the CDAP and is built around
  one or more underlying core or system Datasets to implement a specific data pattern.

- A |custom|_ Dataset is implemented by you and can have arbitrary code and methods.
  It is typically built around one or more Tables (or other Datasets)
  to implement a specific data pattern.

Each Dataset is associated with exactly one Dataset implementation to
manipulate it. Every Dataset has a unique name and metadata that defines its behavior.
For example, every ``IndexedTable`` has a name and indexes a particular column of its primary table:
the name of that column is a metadata property of each Dataset of this type.

.. |core| replace:: **core**
.. _core: index.html#core-datasets

.. |system| replace:: **system**
.. _system: system-custom.html#system-datasets

.. |custom| replace:: **custom**
.. _custom: system-custom.html#custom-datasets


.. _core-datasets:

.. rubric:: Core Datasets

**Tables** are the only core Datasets, and all other Datasets are built using one or more
Tables. These Tables are similar to tables in a relational database with a few key differences:

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

|fileset|_ is a core Dataset type that was added as an experimental feature in CDAP 2.6.0.

.. |fileset| replace:: **FileSet**
.. _fileset: fileset.html

.. rubric::  Examples of Using Datasets

Datasets are included in just about every CDAP :ref:`application <apps-and-packs>`,
:ref:`tutorial <tutorials>`, :ref:`guide <guides-index>` or :ref:`example <examples-index>`.

- The simplest example, :ref:`Hello World <examples-hello-world>`, demonstrates using a
  stream to **ingest a name into a dataset.**

- For examples of **custom datasets,** see the :ref:`Word Count <examples-word-count>`
  and :ref:`Web Analytics <examples-web-analytics>` examples.

- For an example of **a dataset and a Map Reduce Job,** see the :ref:`Purchase
  <examples-purchase>` example.

- For an example of a **Table dataset,** see the :ref:`Word Count <examples-word-count>` example.

- For an example of a **FileSet dataset,** see the :ref:`FileSet <examples-fileset>` example.

- For examples of **key-value Table datasets,** see the
  :ref:`Hello World <examples-hello-world>`,
  :ref:`Count Random <examples-count-random>`,
  :ref:`Word Count <examples-word-count>`, and
  :ref:`Purchase <examples-purchase>` examples.

- For an example of an **ObjectMappedTable dataset,** see the :ref:`Purchase <examples-purchase>` example.

- For examples of **ObjectStore datasets,** see the :ref:`Purchase <examples-purchase>`,
  :ref:`Spark K-Means <examples-spark-k-means>`, and :ref:`Spark Page Rank <examples-spark-page-rank>` examples.

- For an example of a **Timeseries Table dataset,** see the how-to guide :ref:`cdap-timeseries-guide`.

