.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2014 Cask Data, Inc.

============================================
Connecting Flowlets
============================================

There are multiple ways to connect the Flowlets of a Flow. The most
common form is to use the Flowlet name. Because the name of each Flowlet
defaults to its class name, when building the Flow specification you can
simply write::

  .withFlowlets()
    .add(new RandomGenerator())
    .add(new RoundingFlowlet())
  .connect()
    .fromStream("RandomGenerator").to("RoundingFlowlet")

If you have multiple Flowlets of the same class, you can give them explicit names::

  .withFlowlets()
    .add("random", new RandomGenerator())
    .add("generator", new RandomGenerator())
    .add("rounding", new RoundingFlowlet())
  .connect()
    .from("random").to("rounding")
