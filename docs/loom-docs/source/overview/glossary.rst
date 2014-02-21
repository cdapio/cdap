.. _glossary:

========
Glossary
========

.. glossary::

    Provider
        A provider is responsible for providing a machine.  Examples include an Openstack instance, 
        Rackspace, and others. 

    Hardware
        Hardware in Loom refers to a some type of machine, whether it is physical or in the cloud, 
        that a Provider can provide.  

    Image
        An Image in Loom refers to some type of base image that a Provider can provide.  Any software
        that image may contain is not managed by Loom.  Its main use is to specify the OS on a machine.

    Service
        A service in Loom is a piece of software that can be placed on a cluster, and also defines at least one 
        provisioner action that will occur during cluster creation and management.  Provisioner actions include
        install, configure, initialize, start, stop, and remove. 

    Template
        A cluster template is a blueprint describing how clusters should be laid out.  It defines services,
        hardware types, and images types that can be used to create a cluster, as well as default values
        for lease times, services, hardware, image, and provider to be used with the cluster.  In addition,
        a set of constraints is specified to determine how many nodes should have different services, and 
        what services can and cannot exist together on the same node. 

    Lease Time
        A lease time is an optional cluster setting that specifies the amount of time a cluster can live
        before being automatically deleted. 

    Cluster
        A cluster is a group of machines, each of which uses some hardware, image, and at least one service.
        It is created by end users from a template.

    Loom Server
        The Loom Server is a Loom component that stores admin defined providers, hardware types, 
        image types, services, and cluster templates.  It also takes cluster action requests, such
        as requests to create or delete a cluster, and manages what tasks need to happen on what 
        nodes in what order in order to complete those actions. 

    Provisioner
        The provisioner is a Loom component that performs cluster management tasks.  
        It is the piece that performs all the actual work, such as communicating with different
        providers to request machines, installing services, configuring services, etc.  

    Expiry Time
        The expiry time of a cluster indicates the time at which the cluster will be automatically
        deleted.

    Constraints
        Cluster templates contain constraints that describe how services, hardware, and images should 
        come together during cluster creation.  Service constraints can limit what hardware and image
        a service can be placed on, as well as limit a maximum or minimum of that service across the 
        entire cluster.  Layout constraints can force certain service groups to always coexist on the 
        same node, and can force certain service groups to never coexist on the same node.  Constraints
        are local to a cluster template, and can differ across templates.

    Solver
        The solver is the part in the Loom Server that takes a cluster template and a cluster request
        and determines what services to place on what nodes with which image and hardware type.  It is
        also responsible for determining the same when a cluster is scaled up or down.

    Chef
        The Chef

    Lifecycle Hooks
        Hook is 

