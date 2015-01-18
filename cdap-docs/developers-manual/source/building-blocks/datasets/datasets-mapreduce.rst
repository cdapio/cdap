.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2014-2015 Cask Data, Inc.

.. _datasets-mapreduce-programs:

============================================
Datasets and MapReduce Programs
============================================

.. highlight:: java

A MapReduce program can interact with a Dataset by using it as an input or an output.
The Dataset needs to implement specific interfaces to support this.


.. rubric:: A Dataset as the Input Source of a MapReduce Program

When you run a MapReduce program, you can configure it to read its input from a Dataset. The
source Dataset must implement the ``BatchReadable`` interface, which requires two methods::

  public interface BatchReadable<KEY, VALUE> {
    List<Split> getSplits();
    SplitReader<KEY, VALUE> createSplitReader(Split split);
  }

These two methods complement each other: ``getSplits()`` must return all splits of the Dataset
that the MapReduce program will read; ``createSplitReader()`` is then called in every Mapper to
read one of the splits. Note that the ``KEY`` and ``VALUE`` type parameters of the split reader
must match the input key and value type parameters of the Mapper.

Because ``getSplits()`` has no arguments, it will typically create splits that cover the
entire Dataset. If you want to use a custom selection of the input data, define another
method in your Dataset with additional parameters and explicitly set the input in the
``beforeSubmit()`` method.

For example, the system Dataset ``KeyValueTable`` implements ``BatchReadable<byte[], byte[]>``
with an extra method that allows specification of the number of splits and a range of keys::

  public class KeyValueTable extends AbstractDataset
                             implements BatchReadable<byte[], byte[]> {
    ...
    public List<Split> getSplits(int numSplits, byte[] start, byte[] stop);
  }

To read a range of keys and give a hint that you want 16 splits, write::

  @Override
  @UseDataSet("myTable")
  KeyValueTable kvTable;
  ...
  public void beforeSubmit(MapReduceContext context) throws Exception {
    ...
    context.setInput(kvTable, kvTable.getSplits(16, startKey, stopKey);
  }


.. rubric:: A Dataset as the Output Destination of a MapReduce Program

Just as you have the option to read input from a Dataset, you have the option to write to a Dataset as
the output destination of a MapReduce program if that Dataset implements the ``BatchWritable``
interface::

  public interface BatchWritable<KEY, VALUE> {
    void write(KEY key, VALUE value);
  }

The ``write()`` method is used to redirect all writes performed by a Reducer to the Dataset.
Again, the ``KEY`` and ``VALUE`` type parameters must match the output key and value type
parameters of the Reducer.


.. rubric:: Directly Reading and Writing Datasets

Both CDAP ``Mapper`` and ``Reducer`` can :ref:`directly read or write to a Dataset
<mapreduce-datasets>`, similar to the way a Flowlet or Service can.
