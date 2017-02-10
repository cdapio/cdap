.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2014 Cask Data, Inc.

.. _flows-flowlets-index:
.. _flows:

==================
Flows and Flowlets
==================

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

.. toctree::
   :hidden:

   flows
   
*Flows* are user-implemented real-time stream processors. They are comprised of one or
more *Flowlets* that are wired together into a directed acyclic graph or DAG. Flowlets
pass data between one another; each flowlet is able to perform custom logic and execute
data operations for each individual data object it processes. All data operations happen
in a consistent and durable way.

When processing a single input object, all operations, including the
removal of the object from the input, and emission of data to the
outputs, are executed in a transaction. This provides us with Atomicity,
Consistency, Isolation, and Durability (ACID) properties, and helps
assure a unique and core property of the flow system: it guarantees
atomic and "exactly-once" processing of each input object by each
flowlet in the DAG.

Flows are deployed to the CDAP instance and hosted within containers. Each
flowlet instance runs in its own container. Each flowlet in the DAG can
have multiple concurrent instances, each consuming a partition of the
flowlet’s inputs.

To put data into your flow, you can either connect the input of the flow
to a stream, or you can implement a flowlet to generate or pull the data
from an external source.

The ``Flow`` interface allows you to specify the flow’s metadata, :doc:`flowlets,
<flowlets>` :doc:`flowlet connections, <connecting-flowlets>` (either stream to flowlet,
or flowlet to flowlet), and any :ref:`Datasets <datasets-index>` used in the flow.

To create a flow, extend ``AbstractFlow`` and override the ``configure`` method::

  public class MyExampleFlow extends AbstractFlow {

    @Override
    public void configure() {
      setName("mySampleFlow");
      setDescription("Flow for showing examples");
      addFlowlet("flowlet1", new MyExampleFlowlet());
      addFlowlet("flowlet2", new MyExampleFlowlet2());
      connectStream("myStream", "flowlet1");
      connect("flowlet1", "flowlet2");
    }
  }

In this example, the *name*, *description*, *with* (or *without*)
flowlets, and *connections* are specified before building the flow.


Flow and Flowlet Examples
=========================
Flows and flowlets are included in just about every CDAP :ref:`application <apps-and-packs>`,
:ref:`tutorial <tutorials>`, :ref:`guide <guides-index>` or :ref:`example <examples-index>`.

- The simplest example, :ref:`Hello World <examples-hello-world>`, demonstrates using **a
  flow with a single flowlet** to ingest a name into a dataset.

- For an example of **annotated flowlets,** see the :ref:`Count Random
  <examples-count-random>` example.

- For examples of **flows with multiple flowlets,** see the :ref:`Purchase
  <examples-purchase>`, :ref:`Count Random <examples-count-random>`, and
  :ref:`Word Count <examples-word-count>` examples.
  
- The :ref:`Web Analytics <examples-web-analytics>` example uses a single flowlet to
  perform analytics using access logs.
  
- The :ref:`Purchase <examples-purchase>` example demonstrates :ref:`setting the resources
  <flows-flowlets-resources>` used by an individual instance of a flowlet.
