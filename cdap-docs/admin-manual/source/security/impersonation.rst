.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2016 Cask Data, Inc.

.. _configuration-impersonation:

=========================
Configuring Impersonation
=========================

Impersonation allows users to run programs and access datasets, streams, and other resources as pre-configured users.
Currently CDAP supports configuring impersonation at a namespace level, which means that every namespace will have
a single user that programs in that namespace will run as, and that resources will be accessed as.
