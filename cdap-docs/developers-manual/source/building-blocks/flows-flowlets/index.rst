.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2014 Cask Data, Inc.

.. _flows-flowlets-index:
.. _flows:

============================================
Flows and Flowlets
============================================

.. toctree::
   :maxdepth: 1
   :titlesonly:
   
    Flowlets <flowlets>
    Input Context <input-context>
    Type Projection <type-projection>
    Stream Event <stream-event>
    Tick Methods <tick-methods>
    Connecting Flowlets <connecting-flowlets>
    Batch Execution <batch-execution>
    Flowlets and Instances <flowlets-instances>
    Partitioning Strategies <partitioning-strategies>


**Flows** are user-implemented realtime stream processors. They are comprised of one or
more **Flowlets** that are wired together into a directed acyclic graph or DAG. Flowlets
pass data between one another; each Flowlet is able to perform custom logic and execute
data operations for each individual data object it processes. All data operations happen
in a consistent and durable way.

When processing a single input object, all operations, including the
removal of the object from the input, and emission of data to the
outputs, are executed in a transaction. This provides us with Atomicity,
Consistency, Isolation, and Durability (ACID) properties, and helps
assure a unique and core property of the Flow system: it guarantees
atomic and "exactly-once" processing of each input object by each
Flowlet in the DAG.

Flows are deployed to the CDAP instance and hosted within containers. Each
Flowlet instance runs in its own container. Each Flowlet in the DAG can
have multiple concurrent instances, each consuming a partition of the
Flowlet’s inputs.

To put data into your Flow, you can either connect the input of the Flow
to a Stream, or you can implement a Flowlet to generate or pull the data
from an external source.

The ``Flow`` interface allows you to specify the Flow’s metadata, :doc:`Flowlets,
<flowlets>` :doc:`Flowlet connections, <connecting-flowlets>` (either Stream to Flowlet,
or Flowlet to Flowlet), and any :ref:`Datasets <datasets-index>` used in the Flow.

To create a Flow, implement ``Flow`` via a ``configure`` method that
returns a ``FlowSpecification`` using the ``FlowSpecification.Builder()``::

  class MyExampleFlow implements Flow {
    @Override
    public FlowSpecification configure() {
      return FlowSpecification.Builder.with()
        .setName("mySampleFlow")
        .setDescription("Flow for showing examples")
        .withFlowlets()
          .add("flowlet1", new MyExampleFlowlet())
          .add("flowlet2", new MyExampleFlowlet2())
        .connect()
          .fromStream("myStream").to("flowlet1")
          .from("flowlet1").to("flowlet2")
        .build();
  }

In this example, the *name*, *description*, *with* (or *without*)
Flowlets, and *connections* are specified before building the Flow.


.. rubric::  Examples of Using Flows and Flowlets

Flows and Flowlets are included in just about every CDAP :ref:`application <apps-and-packs>`,
:ref:`tutorial <tutorials>`, :ref:`guide <guides-index>` or :ref:`example <examples-index>`.

- The simplest example, :ref:`Hello World <examples-hello-world>`, demonstrates using **a
  flow with a single flowlet** to ingest a name into a dataset.

- For an example of **annotated Flowlets,** see the :ref:`Count Random
  <examples-count-random>` example.

- For examples of **flows with multiple flowlets,** see the :ref:`Purchase
  <examples-purchase>` and :ref:`Count Random <examples-count-random>` examples.
