
.. :Author: Cask Data, Inc.
   :Description: Cask Data Application Platform CountRandom Application
   :copyright: Copyright © 2014 Cask Data, Inc.

.. _count-random:

Count Random
------------

A Cask Data Application Platform (CDAP) Example demonstrating the @Tick feature of Flows.

Overview
........

This application does not have a Stream, instead it uses a Tick annotation in the ``source`` flowlet to generate data:

- The ``generate`` method of the  ``source`` flowlet has a ``@Tick`` annotation which specifies how frequently the method will be called.
- The ``source`` flowlet generates a random integer in the range {1..10000} and emits it to the next flowlet ``splitter``.
- The ``splitter`` flowlet splits the number into digits, and emits these digits to the next stage.
- The ``counter`` increments the count of the received number in the KeyValueTable.

Let's look at some of these elements, and then run the Application and see the results.

The Count Random Application
............................

.. highlight:: java

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

The Flowlet that generates random numbers every 1 millisecond::

  public class RandomSource extends AbstractFlowlet {
    private OutputEmitter<Integer> randomOutput;

    private final Random random = new Random();

    @Tick(delay = 1L, unit = TimeUnit.MILLISECONDS)
    public void generate() throws InterruptedException {
      randomOutput.emit(random.nextInt(10000));
    }
  }


Deploy and start the Application as described in  :ref:`Building and Running Applications <convention>`

Running the Application
+++++++++++++++++++++++

Once you start the flow, the ``source`` flowlet will continuously generate data. You can see this by observing the counters
for each flowlet in the flow visualization. Even though you are not injecting any data into the flow, the counters increase steadily.

Once done, you can stop the application as described in :ref:`Building and Running Applications <stop-application>`.

