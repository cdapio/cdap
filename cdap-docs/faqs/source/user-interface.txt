.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2015 Cask Data, Inc.

:titles-only-toc: true

.. _faq-user-interface:

========================
CDAP FAQ: User Interface
========================

.. contents::
   :depth: 2
   :local:
   :backlinks: entry
   :class: faq

User Interface
==============

Does CDAP support CORS?
-----------------------
CORS (`Cross-Origin Resource Sharing <http://www.w3.org/TR/cors/>`__) is 
currently not supported in CDAP. 

If you were interested in using CORS to create a webapp that showed information about CDAP
gathered through the RESTful APIs, a workaround would be the method used for the CDAP UI.
Make backend requests through a `Node.js <https://nodejs.org/>`__ server and route the
response to the client browser. Here, the Node.js server acts as a proxy and from it you
can call the CDAP RESTful end points without any issues of cross-domain.

Additional Resources
====================

Ask the CDAP Community for assistance
-------------------------------------

.. include:: cdap-user-googlegroups.txt
