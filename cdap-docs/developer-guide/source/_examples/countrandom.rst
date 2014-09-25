:orphan:

.. :Author: Cask Data, Inc.
   :Description: Cask Data Application Platform CountRandom Application
       :copyright: Copyright © 2014 Cask Data, Inc.

.. _count-random:

Count Random
------------

A Cask Data Application Platform (CDAP) Example demonstrating Flows.

Overview
........

This application does not have a Stream, instead it has a Generator Flowlet ``source``
  - The ``source`` flowlet has a ``@Tick`` annotation which specifies how frequent this flowlet will be called.
  - The ``source`` flowlet generates a random integer in the range {1..10000} and emits it to the next flowlet ``splitter``
  - The ``splitter`` flowlet splits the number into digits, and emits these digits to the next stage
  - The ``counter`` increments the count of the received number in the KeyValueTable.

Let's look at some of these elements, and then run the Application and see the results.

The Count Random Application
............................

As in the other `examples <index.html>`__, the components
of the Application are tied together by the class ``CountRandom``::

  public class CountRandom extends AbstractApplication {

    public static final String TABLE_NAME = "randomTable";

    @Override
    public void configure() {
      setName("CountRandom");
      setDescription("Example random count application");
      createDataset(TABLE_NAME, KeyValueTable.class);
      addFlow(new CountRandomFlow());
    }
  }

The Generator Flowlet that generates Random numbers every 1 millisecond::

  public class RandomSource extends AbstractFlowlet {
    private OutputEmitter<Integer> randomOutput;

    private final Random random = new Random();

    @Tick(delay = 1L, unit = TimeUnit.MILLISECONDS)
    public void generate() throws InterruptedException {
      randomOutput.emit(random.nextInt(10000));
    }
  }


You can find instructions for starting CDAP console and deploying an example application here :ref:`Build, Deploy and start <convention>`
Once Deployed, select the ``CountRandom`` Application from the list.
On the Application's detail page, click the *Start* button on the *Process* list.

Viewing the Run:
++++++++++++++++

Once the flow ``source`` is started, you could see the stream count is '0', however the ``splitter`` would show the count of random numbers
received from the source and the ``counter`` will show the count of digits received.

Stopping the Application
++++++++++++++++++++++++

Either:

- On the Application detail page of the CDAP Console,
  click the *Stop* button on the *Process* list;

or:

- Run ``$ ./bin/app-manager.sh --action stop``

  On Windows, run ``~SDK> bin\app-manager.bat stop``

