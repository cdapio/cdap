.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2014-2015 Cask Data, Inc.

.. _datasets-mapreduce-programs:

===============================
Datasets and MapReduce Programs
===============================

.. highlight:: java

A MapReduce program can interact with a dataset by using it as an input or an output.
The dataset needs to implement specific interfaces to support this.


.. rubric:: A Dataset as the Input Source of a MapReduce Program

When you run a MapReduce program, you can configure it to read its input from a dataset. The
source dataset must implement the ``BatchReadable`` interface, which requires two methods::

  public interface BatchReadable<KEY, VALUE> {
    List<Split> getSplits();
    SplitReader<KEY, VALUE> createSplitReader(Split split);
  }

These two methods complement each other: ``getSplits()`` must return all splits of the dataset
that the MapReduce program will read; ``createSplitReader()`` is then called in every Mapper to
read one of the splits. Note that the ``KEY`` and ``VALUE`` type parameters of the split reader
must match the input key and value type parameters of the Mapper.

Because ``getSplits()`` has no arguments, it will typically create splits that cover the
entire dataset. If you want to use a custom selection of the input data, define another
method in your dataset with additional parameters and explicitly set the input in the
``beforeSubmit()`` method.

For example, the system dataset ``KeyValueTable`` implements ``BatchReadable<byte[], byte[]>``
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
    context.setInput("myTable", kvTable.getSplits(16, startKey, stopKey));
  }


.. rubric:: A Dataset as the Output Destination of a MapReduce Program

Just as you have the option to read input from a dataset, you have the option to write to a dataset as
the output destination of a MapReduce program if that dataset implements the ``BatchWritable``
interface::

  public interface BatchWritable<KEY, VALUE> {
    void write(KEY key, VALUE value);
  }

The ``write()`` method is used to redirect all writes performed by a Reducer to the dataset.
Again, the ``KEY`` and ``VALUE`` type parameters must match the output key and value type
parameters of the Reducer.


.. rubric:: Multiple Output Destinations of a MapReduce Program

To write to multiple output datasets from a MapReduce program, begin by adding the datasets as outputs::

  public void beforeSubmit(MapReduceContext context) throws Exception {
    ...
    context.addOutput("productCounts");
    context.addOutput("catalog");
  }

Then, have the ``mapper`` and/or ``reducer`` implement ``ProgramLifeCycle<MapReduceTaskContext>``. 
This is to obtain access to the ``MapReduceTaskContext`` in their initialization methods and 
to be able to write using the write method of the ``MapReduceTaskContext``::

  public static class CustomMapper extends Mapper<LongWritable, Text, NullWritable, Text>
    implements ProgramLifecycle<MapReduceTaskContext<NullWritable, Text>> {

    private MapReduceTaskContext<NullWritable, Text> mapReduceTaskContext;

    @Override
    public void initialize(MapReduceTaskContext<NullWritable, Text> context) throws Exception {
      this.mapReduceTaskContext = context;
    }

    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
      // compute some condition
      ...
      if (someCondition) {
        mapReduceTaskContext.write("productCounts", key, value);
      } else {
        mapReduceTaskContext.write("catalog", key, value);
      }
    }

  }

Note that the multiple output write method |---| ``MapReduceTaskContext.write(String, KEY key, VALUE value)`` |---| can
only be used if there are multiple outputs. Similarly, the single output write
method |---| ``MapReduceTaskContext.write(KEY key, VALUE value)`` |---| can only be used if there
is a single output to the MapReduce program.

.. rubric:: Directly Reading and Writing Datasets

Both CDAP ``mapper`` and ``reducer`` can :ref:`directly read or write to a dataset
<mapreduce-datasets>`, similar to the way a flowlet or service can.
