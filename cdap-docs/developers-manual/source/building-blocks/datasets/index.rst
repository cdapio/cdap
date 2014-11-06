.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2014 Cask Data, Inc.

.. _datasets-index:

============================================
Datasets
============================================

.. toctree::
   :maxdepth: 1
   
    Types of Datasets <types>
    Core Datasets <core>
    Table API <table>
    System and Custom Datasets <system-custom>
    Datasets and MapReduce <datasets-mapreduce>

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

You can create a Dataset in CDAP using either the
:ref:`Cask Data Application Platform HTTP RESTful API <http-restful-api-datasets>` or command-line tools.

You can also tell Applications to create a Dataset if it does not already
exist by declaring the Dataset details in the Application specification.
For example, to create a DataSet named *myCounters* of type 
`KeyValueTable <javadocs/co/cask/cdap/api/dataset/lib/KeyValueTable.html>`__, write::

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
