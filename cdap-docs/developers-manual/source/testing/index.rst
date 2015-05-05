.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2014 Cask Data, Inc.

:hide-toc: true

============================================
Testing and Debugging
============================================

.. toctree::
   :maxdepth: 1
   
    Testing <testing>
    Debugging <debugging>
    Troubleshooting <troubleshooting>

..    Packaging <packaging>


CDAP comes with a number of tools to make a developer's life easier. These tools
help with testing and debugging CDAP applications:

.. |test-framework| replace:: **Test Framework:**
.. _test-framework: testing.html#test-framework

- |test-framework|_ How you can take advantage of the **test framework** to test your CDAP
  applications before deploying. This makes catching bugs early and easy.


.. |debugging| replace:: **Debugging:**
.. _debugging: debugging.html

- |debugging|_ How you can **debug CDAP applications** in Standalone mode and app
  containers in Distributed mode.


.. |debugging-tx| replace:: **Debugging the Transactions Manager:**
.. _debugging-tx: debugging.html#tx-debugger

- |debugging-tx|_ Covers snapshotting and inspecting the state of the **Transaction Manager**.


.. |troubleshooting| replace:: **Troubleshooting:**
.. _troubleshooting: troubleshooting.html

- |troubleshooting|_ Tips and hints on solving problems during development.