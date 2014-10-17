.. :author: Cask Data, Inc.
   :copyright: Copyright © 2014 Cask Data, Inc.

============================================
Putting CDAP into Production
============================================

The Cask Data Application Platform (CDAP) can be run in different modes: in-memory mode
for unit testing, Standalone CDAP for testing on a developer's laptop, and Distributed
CDAP for staging and production.

Regardless of the runtime edition, CDAP is fully functional and the code you develop never
changes. However, performance and scale are limited when using in-memory or standalone
CDAPs.

In-memory CDAP
--------------
The in-memory CDAP allows you to easily run CDAP for use in unit tests. In this mode, the
underlying Big Data infrastructure is emulated using in-memory data structures and there
is no persistence. The CDAP Console is not available in this mode.

Standalone CDAP
---------------

The Standalone CDAP allows you to run the entire CDAP stack in a single Java Virtual
Machine on your local machine and includes a local version of the CDAP Console. The
underlying Big Data infrastructure is emulated on top of your local file system. All data
is persisted.

The Standalone CDAP by default binds to the localhost address, and is not available for
remote access by any outside process or application outside of the local machine.

See the :ref:`Getting Started Guide <get-started>` and the *Cask Data Application Platform
SDK* for information on how to start and manage your Standalone CDAP.


Distributed Data Application Platform
-------------------------------------

The Distributed CDAP runs in fully distributed mode. In addition to the system components
of the CDAP, distributed and highly available deployments of the underlying Hadoop
infrastructure are included. Production applications should always be run on a Distributed
CDAP.

To learn more about getting your own Distributed CDAP, see `Cask Products
<http://cask.co/products>`__.

