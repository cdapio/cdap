.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2014 Cask Data, Inc.

===================
Connecting Flowlets
===================

There are multiple ways to connect the flowlets of a flow. The most
common form is to use the flowlet name. Because the name of each flowlet
defaults to its class name, when building the flow specification you can
simply write::

  .withFlowlets()
    .add(new RandomGenerator())
    .add(new RoundingFlowlet())
  .connect()
    .fromstream("RandomGenerator").to("RoundingFlowlet")

If you have multiple flowlets of the same class, you can give them explicit names::

  .withFlowlets()
    .add("random", new RandomGenerator())
    .add("generator", new RandomGenerator())
    .add("rounding", new RoundingFlowlet())
  .connect()
    .from("random").to("rounding")
