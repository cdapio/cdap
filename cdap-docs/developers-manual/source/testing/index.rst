.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2014-2017 Cask Data, Inc.

:hide-toc: true

=====================
Testing and Debugging
=====================

.. toctree::
   :maxdepth: 1

    Testing a CDAP Application <testing>
    Debugging <debugging>
    Troubleshooting <troubleshooting>


CDAP comes with a number of tools to make a developer's life easier. These tools
help with testing and debugging CDAP applications:

.. |testing-cdap| replace:: **Testing a CDAP Application:**
.. _testing-cdap: testing.html

- |testing-cdap|_ Strategies and approaches for **testing CDAP applications**, including
  testing flows, Map Reduce and Spark programs, and artifacts.


.. |test-framework| replace:: **Test Framework:**
.. _test-framework: testing.html#test-framework

- |test-framework|_ How you can take advantage of the **test framework** and the in-memory
  CDAP mode to test your CDAP applications before deploying. This makes catching bugs early
  and easy.


.. |debugging| replace:: **Debugging:**
.. _debugging: debugging.html

- |debugging|_ How you can **debug CDAP applications** in CDAP Local Sandbox mode and application
  containers in Distributed CDAP mode.


.. |debugging-tx| replace:: **Debugging the Transactions Manager:**
.. _debugging-tx: debugging.html#tx-debugger

- |debugging-tx|_ Covers snapshotting and inspecting the state of the **Transaction Manager**.


.. |troubleshooting| replace:: **Troubleshooting:**
.. _troubleshooting: troubleshooting.html

- |troubleshooting|_ Tips and hints on solving problems during development.
