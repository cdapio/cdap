.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2016 Cask Data, Inc.

.. _admin-secure-storage:

==========================
Configuring Secure Storage
==========================

**Secure Storage** allows users to store sensitive information such as passwords in an encrypted manner.

**Note:** In CDAP 3.5.0, encryption and decryption of the contents only happens at the
secure store, not while the data is transitting to the secure store. In a later version of
CDAP, all transport involving secure keys will be secured using SSL.
