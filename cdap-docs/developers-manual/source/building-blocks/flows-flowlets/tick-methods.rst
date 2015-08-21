.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2014 Cask Data, Inc.

============
Tick Methods
============

A flowlet’s method can be annotated with ``@Tick``. Instead of
processing data objects from a flowlet input, this method is invoked
periodically, without arguments. This can be used, for example, to
generate data, or pull data from an external data source periodically on
a fixed cadence.

In this code snippet from the *CountRandom* example, the ``@Tick``
method in the flowlet emits random numbers::

  public class RandomSource extends AbstractFlowlet {

    private OutputEmitter<Integer> randomOutput;

    private final Random random = new Random();

    @Tick(delay = 1L, unit = TimeUnit.MILLISECONDS)
    public void generate() throws InterruptedException {
      randomOutput.emit(random.nextInt(10000));
    }
  }

Note: ``@Tick`` method calls are serial; subsequent calls to the tick
method will be made only after the previous ``@Tick`` method call has returned.

