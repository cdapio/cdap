.. :Author: Continuuity, Inc 
   :Description: Release notes for Continuuity Reactor

.. _overview_release-notes:

.. index::
   single: Release Notes

=============
Release Notes
=============
.. _release-notes:

New Features
^^^^^^^^^^^^^
- **Stream data ingestion improvements:**
  New implementation of Streams enables increased throughput and horizontal scalability, improving data ingestion performance.
- **Consolidation of Reactor services:**
  Reactor services are consolidated so that dev-ops would need to operationalize fewer services. 
- **Introspection of transaction states:**
  Added capabilities to dump the state of the internal transaction manager into a local file to allow additional investigation.
- **Expose runtime information of programs running in YARN:**
  Program runtime information can be obtained through an HTTP end-point.

Major Bug Fixes
^^^^^^^^^^^^^^^
• Making metrics services resilient to Kafka server restarts
• Fixes to ensure resource view metrics being displayed correctly
• Making Flowlets resilient to transaction services restarts
• Fixes to avoid NPE on transaction services shutdown
• Programs to utilize runtime arguments when started from REST end-points
• Better display for runtime configurations in the Reactor dashboard
• Capabilities to deploy program jar files without a size limitation
• Improvements to pruning invalid transactions

