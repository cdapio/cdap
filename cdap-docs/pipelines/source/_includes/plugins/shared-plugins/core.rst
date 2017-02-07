.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2016-2017 Cask Data, Inc.

.. _cdap-pipelines-plugins-shared-core-validator:

=============
CoreValidator
=============

.. rubric:: Description

A system-supplied validator that offers a set of functions that can be called from the Validator transform.

It is included in a transform by adding its name (``core``) to the ``validators`` field of
the transform configuration and its functions are referenced by using its JavaScript name
(``coreValidator``). See an example in the `Validator transforms plugin <../transforms/validator-transform.html>`__.

.. rubric:: Use Case

Users often want to validate the input for a certain data-type or format or to check if
they are a valid date, credit card, etc. It's useful to implement and aggregate these
common functions and make them easily available.

.. rubric:: Functions

This table lists the methods available in ``CoreValidator`` that can be called from the ``ValidatorTransform``:

.. Imports functions found in the file Validator-transform.md

.. include:: /../target/_includes/plugins/_includes/validator-extract.txt

CDAP Pipelines Version |cdap-pipelines-version|
