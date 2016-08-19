.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2014 Cask Data, Inc.

===============
Batch Execution
===============

By default, a flowlet processes a single data object at a time within a single
transaction. To increase throughput, you can also process a batch of data objects within
the same transaction::

  @Batch(100)
  @ProcessInput
  public void process(String words) {
    ...

For the above batch example, the **process** method will be called up to 100 times per
transaction, with different data objects read from the input each time it is called.

If you are interested in knowing when a batch begins and ends, you can use an **Iterator**
as the method argument::

  @Batch(100)
  @ProcessInput
  public void process(Iterator<String> words) {
    ...

In this case, the **process** will be called once per transaction and the **Iterator**
will contain up to 100 data objects read from the input.

The batch size can also be controlled through flowlet properties or runtime arguments. You can specify the *key*
to use for looking up the batch size value with a ``@Batch`` annotation::

  @Batch(key = "batch.size", value = 100)
  @ProcessInput
  public void process(String word) {
    ...
  }

By specifying the **key** element in the ``@Batch`` annotation, the runtime system will resolve the
batch size in this order (highest precedence first):

1. Runtime argument with name = **flowlet.<flowletName>.<key>**
#. Runtime argument with name = **flowlet.\*.<key>**
#. Runtime argument with name = **<key>**
#. Flowlet properties with name = **<key>**
#. The **value** element specified in the ``@Batch`` annotation
