.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2014 Cask Data, Inc.

.. _flows-flowlets-instances:
.. _flows-flowlets-resources:

===============================
Flowlet Instances and Resources
===============================

Both the number of instances and the resources it requires can be specified for a flowlet.

Instances
=========
You can have one or more instances of any given flowlet, each consuming a disjoint
partition of each input. You can control the number of instances programmatically via the
:ref:`REST interfaces <rest-scaling-flowlets>` or via the CDAP UI. This enables you
to scale your application to meet capacity at runtime.

In the stand-alone CDAP, multiple flowlet instances are run in threads, so in some cases
actual performance may not be improved. However, in the Distributed CDAP,
each flowlet instance runs in its own Java Virtual Machine (JVM) with independent compute
resources. Scaling the number of flowlets can improve performance and have a major impact
depending on your implementation.

Resources
=========

When an application is configured, the YARN container for a flowlet can be sized, both in terms of
the amount of memory and the number of virtual cores assigned.

For instance, using the example of the :ref:`RoundingFlowlet <flowlets_RoundingFlowlet>`, the size of
the YARN container (in megabytes) and the number of virtual cores can be set by using the Resources API
to create a Resource instance for the flowlet::

  public class RoundingFlowlet extends AbstractFlowlet {

    @Override
    public void configure(FlowletConfigurer configurer) {
      super.configure(configurer);
      setName("round");
      setDescription("A rounding flowlet");
      setResources(new Resources(1024, 2));
    }

If only the memory requirement needs to be set, that can be done using::

    setResources(new Resources(1024));

This is shown in the :ref:`Purchase <examples-purchase>` example, in the configuration of
the ``PurchaseStore`` flowlet.
