.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2014 Cask Data, Inc.

============================================
Concepts and Components of CDAP
============================================

.. rubric:: Components of CDAP and their Interactions

This diagram illustrates the components that comprise Distributed CDAP and shows some of their interactions,
with CDAP System components in orange and non-system components in yellow:

.. image:: ../_images/arch_components_view.png
   :width: 6in
   :align: center

CDAP consists chiefly of these components:

- **The Router** is the only public access point into CDAP for external clients. It forwards client requests to
  the appropriate system service or application. In a secure setup, the router also performs authentication;
  it is then complemented by an authentication service that allows clients to obtain access tokens for CDAP.
  
- **The Master** controls and manages all services and applications.

- **System Services** provide vital platform features such datasets, transactions, service discovery logging,
  and metrics collection. System services run in application containers.
  
- **Application containers** provide abstraction and isolation for execution of application code (and, as a
  special case, system services). Application containers scale linearly and elastically with the underlying
  infrastructure.

In a Hadoop Environment, application containers are implemented as YARN containers and datasets use HBase and
HDFS for actual storage. In other environments, the implementation can be different. For example, in Standalone
CDAP, all services run in a single JVM, application containers are implemented as threads, and data is stored in
the local file system.
