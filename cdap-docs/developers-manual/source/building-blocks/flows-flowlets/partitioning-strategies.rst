.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2014 Cask Data, Inc.

=======================
Partitioning Strategies
=======================

As mentioned above, if you have multiple instances of a flowlet the input queue is
partitioned among the flowlets. The partitioning can occur in different ways, and each
flowlet can specify one of these three partitioning strategies:

- **First-in first-out (FIFO):** Default mode. In this mode, every flowlet instance
  receives the next available data object in the queue. However, since multiple consumers
  may compete for the same data object, access to the queue must be synchronized. This may
  not always be the most efficient strategy.

- **Round-robin:** With this strategy, the number of items is distributed evenly among the
  instances. In general, round-robin is the most efficient partitioning. Though more
  efficient than FIFO, it is not ideal when the application needs to group objects into
  buckets according to business logic. In those cases, hash-based partitioning is
  preferable.

- **Hash-based:** If the emitting flowlet annotates each data object with a hash key, this
  partitioning ensures that all objects of a given key are received by the same consumer
  instance. This can be useful for aggregating by key, and can help reduce write conflicts.

.. rubric:: First-in first-out (FIFO) Partitioning

Suppose we have a flowlet that counts words::

  public class Counter extends AbstractFlowlet {

    @UseDataSet("wordCounts")
    private KeyValueTable wordCountsTable;

    @ProcessInput("wordOut")
    public void process(String word) {
      this.wordCountsTable.increment(Bytes.toBytes(word), 1L);
    }
  }

This flowlet uses the default strategy of FIFO. 

.. rubric:: Round-robin Partitioning

To increase the throughput when this flowlet has many instances, we can specify
round-robin partitioning::

  @RoundRobin
  @ProcessInput("wordOut")
  public void process(String word) {
    this.wordCountsTable.increment(Bytes.toBytes(word), 1L);
  }

Now, if we have three instances of this flowlet, every instance will receive every third
word. For example, for the sequence of words in the sentence, “I scream, you scream, we
all scream for ice cream”:

- The first instance receives the words: *I scream scream cream*
- The second instance receives the words: *scream we for*
- The third instance receives the words: *you all ice*

The potential problem with this is that the first two instances might
both attempt to increment the counter for the word *scream* at the same time,
leading to a write conflict. 

.. rubric:: Hash-based Partitioning

To avoid conflicts, we can use hash-based partitioning::

  @HashPartition("wordHash")
  @ProcessInput("wordOut")
  public void process(String word) {
    this.wordCountsTable.increment(Bytes.toBytes(word), 1L);
  }

Now only one of the flowlet instances will receive the word *scream*, and there can be no
more write conflicts. Note that in order to use hash-based partitioning, the emitting
flowlet must annotate each data object with the partitioning key::

  @Output("wordOut")
  private OutputEmitter<String> wordOutput;
  ...
  public void process(StreamEvent event) {
    ...
    // emit the word with the partitioning key name "wordHash"
    wordOutput.emit(word, "wordHash", word.hashCode());
  }

Note that the emitter must use the same name ("wordHash") for the key that the consuming
flowlet specifies as the partitioning key. If the output is connected to more than one
flowlet, you can also annotate a data object with multiple hash keys—each consuming
flowlet can then use different partitioning. This is useful if you want to aggregate by
multiple keys, such as counting purchases by product ID as well as by customer ID.

.. rubric:: Partitioning and Batch Execution

Partitioning can be combined with batch execution::

  @Batch(100)
  @HashPartition("wordHash")
  @ProcessInput("wordOut")
  public void process(Iterator<String> words) {
     ...
