.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2014 Cask Data, Inc.

============================================
Flowlets and Instances
============================================

You can have one or more instances of any given Flowlet, each consuming a disjoint
partition of each input. You can control the number of instances programmatically via the
:ref:`REST interfaces <rest-scaling-flowlets>` or via the CDAP UI. This enables you
to scale your application to meet capacity at runtime.

In the stand-alone CDAP, multiple Flowlet instances are run in threads, so in some cases
actual performance may not be improved. However, in the Distributed CDAP,
each Flowlet instance runs in its own Java Virtual Machine (JVM) with independent compute
resources. Scaling the number of Flowlets can improve performance and have a major impact
depending on your implementation.