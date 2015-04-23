.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2014 Cask Data, Inc.

===========================
CDAP Programming Interfaces
===========================

We distinguish between the Developer Interface and the Client Interface.

- The Developer Interface is used to build applications and exposes various Java APIs that are only available to
  code that runs inside application containers. Examples are the Dataset and Transaction APIs as well as the
  various supported programming paradigms.
- The Client interface is a RESTful API and the only way that external clients can interact with CDAP and
  applications. It includes APIs that are not accessible from inside containers, such as application
  lifecycle management and monitoring. As an alternative to HTTP, clients can also use the client libraries
  provided for different programming languages, which include Java, Python, and Ruby.

..  provided for different programming languages, which include Java, JavaScript, and Python.

.. image:: ../_images/arch_interfaces.png
   :width: 5in
   :align: center

Note that certain interfaces are included in both the Developer and the Client APIs; examples are Service Discovery
and Dataset Administration.
