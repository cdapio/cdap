.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2015 Cask Data, Inc.

.. _test-cdap:

================================================
Testing a CDAP Application
================================================

.. _test-framework:

Strategies in Testing Applications: Test Framework
==================================================

CDAP comes with a convenient way to unit test your Applications with CDAP’s Test Framework.
The base for these tests is ``TestBase``, which is packaged
separately from the API in its own artifact because it depends on the
CDAP’s runtime classes. You can include it in your test dependencies
in one of two ways:
