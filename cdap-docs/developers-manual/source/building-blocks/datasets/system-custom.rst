.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2014 Cask Data, Inc.

============================================
System and Custom Datasets
============================================

.. _system-datasets:

System Datasets
============================================

The Cask Data Application Platform comes with several system-defined Datasets, including but not limited to
key/value Tables, indexed Tables and time series. Each of them is defined with the help of one or more embedded
Tables, but defines its own interface. Examples include:

- The ``KeyValueTable`` implements a key/value store as a Table with a single column.

- The ``IndexedTable`` implements a Table with a secondary key using two embedded Tables,
  one for the data and one for the secondary index.

- The ``TimeseriesTable`` uses a Table to store keyed data over time
  and allows querying that data over ranges of time.

- The ``ObjectMappedTable`` uses a Table to store Java Objects by mapping object fields to
  table columns. It can be explored through the use of ad-hoc SQL-like queries as described
  in :ref:`ObjectMappedTable Exploration <object-mapped-table-exploration>`.

See the :ref:`Javadocs <reference:javadocs>` for these classes and the :ref:`Examples <examples-index>`
to learn more about these Datasets. Any class in the CDAP libraries that implements the ``Dataset`` interface is a
system Dataset.


.. _custom-datasets:

Custom Datasets
============================================

.. highlight:: java

You can define your own Dataset classes to implement common data patterns specific to your code.

Suppose you want to define a counter table that, in addition to counting words,
counts how many unique words it has seen. The Dataset can be built on top of two underlying Datasets. The first a
Table (``entryCountTable``) to count all the words and the second a Table (``uniqueCountTable``) for the unique count.

When your custom Dataset is built on top of one or more existing Datasets, the simplest way to implement
it is to just define the data operations (by implementing the Dataset interface) and delegating all other
work (such as  administrative operations) to the embedded Dataset.

To do this, you need to implement the Dataset class and define the embedded Datasets by annotating
its constructor arguments.

In this case, our  ``UniqueCountTableDefinition`` will have two underlying Datasets:
an ``entryCountTable`` and an ``uniqueCountTable``, both of type ``Table``::

  public class UniqueCountTable extends AbstractDataset {

    private final Table entryCountTable;
    private final Table uniqueCountTable;

    public UniqueCountTable(DatasetSpecification spec,
                            @EmbeddedDataset("entryCountTable") Table entryCountTable,
                            @EmbeddedDataset("uniqueCountTable") Table uniqueCountTable) {
      super(spec.getName(), entryCountTable, uniqueCountTable);
      this.entryCountTable = entryCountTable;
      this.uniqueCountTable = uniqueCountTable;
    }

In this case, the class must have one constructor that takes a ``DatasetSpecification`` as a first
parameter and any number of ``Dataset``\s annotated with the ``@EmbeddedDataset`` annotation as the
remaining parameters. ``@EmbeddedDataset`` takes the embedded Dataset's name as a parameter.

The ``UniqueCountTable`` stores a counter for each word in its own row of the entry count table.
For each word the counter is incremented. If the result of the increment is 1, then this is the first time
we've encountered that word, hence we have a new unique word and we then increment the unique counter::

    public void updateUniqueCount(String entry) {
      long newCount = entryCountTable.incrementAndGet(new Increment(entry, "count", 1L)).getInt("count");
      if (newCount == 1L) {
        uniqueCountTable.increment(new Increment("unique_count", "count", 1L));
      }
    }

Finally, we write a method to retrieve the number of unique words seen::

    public Long readUniqueCount() {
      return uniqueCountTable.get(new Get("unique_count", "count")).getLong("count");
    }


All administrative operations (such as create, drop, truncate) will be delegated to the embedded Datasets
in the order they are defined in the constructor. ``DatasetProperties`` that are passed during creation of
the Dataset will be passed as-is to the embedded Datasets.

To create a Dataset of type ``UniqueCountTable``, add the following into the Application implementation::

  Class MyApp extends AbstractApplication {
    public void configure() {
      createDataset("myCounters", UniqueCountTable.class)
      ...
    }
  }

You can also pass ``DatasetProperties`` as a third parameter to the ``createDataset`` method.
These properties will be used by embedded Datasets during creation and will be available via the
``DatasetSpecification`` passed to the Dataset constructor. For example, to create a Dataset with
a TTL (time-to-live) property, you can use::

  createDataset("frequentCustomers", KeyValueTable.class,
    DatasetProperties.builder()
                     .add(Table.PROPERTY_TTL, "3600")      
                     .build());

You can pass other properties, such as for 
:ref:`conflict detection <transaction-system-conflict-detection>` and for
:ref:`pre-splitting into multiple regions <table-datasets-pre-splitting>`.

Application components can access a created Dataset via the ``@UseDataSet`` annotation::

  Class MyFlowlet extends AbstractFlowlet {
    @UseDataSet("myCounters")
    private UniqueCountTable counters;
    ...
  }

A complete application demonstrating the use of a custom Dataset is included in our
:ref:`Purchase Example. <examples-purchase>`

You can also create, drop, and truncate Datasets using the :ref:`http-restful-api-dataset`.
