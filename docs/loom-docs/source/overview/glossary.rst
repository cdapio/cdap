.. _glossary:
.. index::
   single: Glossary
========
Glossary
========

.. glossary::

    Administrator
        An administrator of Loom defines the various configuration elements for creating clusters in Loom.
        These include settings such as the provider to host the cluster, the hardware configuration of the nodes,
        the base disk image of nodes, the software services installed on the cluster, and the duration of lease
        on the clusters. These settings are all tied together via templates that can be used by Loom
        users to create clusters. In addition, an administrator can monitor, manage and delete the clusters
        created across all users.

    Catalog
         A catalog is a list containing the templates created by the administrator. All templates in the catalog are
         available to users for cluster creation. The catalog can be accessed through the Admin UI or
         through calls to the REST API. For a  user, the catalog is displayed under 'Template' when creating
         a new cluster.

    Chef
        Chef is an automation platform that transforms infrastructure configurations into code. It is used to simplify
        server maintenance and creation tasks, and is especially useful for repeatability in server configurations.

    Cluster
        A cluster is a group of machines, each of which uses some hardware, image, and at least one service.
        It is created by end users from a template.

    Constraints
        Cluster templates contain constraints that describe how services, hardware, and images should
        come together during cluster creation. Service constraints can limit what hardware and image
        a service can be placed on a cluster, as well as limit the maximum or minimum instances of that service across the
        entire cluster. Layout constraints can force certain service groups to always coexist on the
        same node, and can force certain service groups to never coexist on the same node. Constraints
        are local to a cluster template, and can differ across templates.

    Expiry Time
        The expiry time of a cluster indicates the time at which the cluster will be automatically
        deleted.

    Hardware
        Hardware in Loom refers to a type of machine, either physical or in the cloud, that a Provider can provide.


    Image
        An Image in Loom refers to some type of base image that a Provider can provide. Any software
        that the image may contain is not managed by Loom. Its main use is to specify the OS on a machine.

    Lease Time
        A lease time is an optional cluster setting that specifies the amount of time a cluster can run for
        before being automatically deleted.

    Loom Server
        The Loom Server is a Loom component that stores admin defined providers, hardware types,
        image types, services, and cluster templates. Loom server takes cluster action requests, such
        as requests to create or delete a cluster. It also manages the tasks each node need to run,
        as well as the order of tasks to complete the actions.

    Provider
        A provider is responsible for providing a machine. Examples include an Openstack instance,
        Rackspace, and Amazon EC2.

    Provisioner
        The provisioner is a Loom component that performs cluster management tasks.
        It performs all the actual tasks, including communicating with different
        providers to request machines, installing services, and configuring services.

    Service
        A service in Loom is a piece of software that can be made available on a cluster. It defines at least one
        provisioner action that will occur during cluster creation and management. Provisioner actions fall
        into one of the following types: install, configure, initialize, start, stop, and remove.

    Solver
        The solver is the component in Loom Server that takes a cluster template and a cluster request,
        and determines the services to place on each node, based on their image and hardware type. The solver
        also performs similar tasks when a cluster is scaled up or down.

    Template
        A cluster template is a blueprint describing how clusters should be laid out. It defines services,
        hardware types, and images types that can be used to create a cluster, as well as default values
        for lease times, services, hardware, image, and provider to be used with the cluster. In addition,
        a set of constraints is specified to determine how many nodes should have different services, and
        what services can and cannot exist together on the same node.

    User
        A user of Loom can create clusters through Loom within the permitted configurations set
        by the Administrator, as specified through the templates in the catalog.

    Zookeeper
         ZooKeeper is a centralized service for maintaining metadata, naming, providing distributed synchronization,
         and providing group services. ZooKeeper provides a distributed system that allows decisions to be made by
         quorum.

