.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2016 Cask Data, Inc.
    :description: FAQ, Frequently Asked Questions and terms related to Cask Hydrator, ETL, and Data Pipelines

:titles-only-global-toc: true

.. _cask-hydrator-faqs:

===================
FAQs: Cask Hydrator
===================

.. contents:: 
    :local:
    :backlinks: none

Is Cask Hydrator open source?
-----------------------------
Yes, Cask Hydrator is open source, licensed under the Apache 2.0 license.
 
Can I only install Cask Hydrator with CDAP?
-------------------------------------------
Cask Hydrator is an extension to CDAP and it uses the APIs of CDAP to create, deploy, and
manage data pipelines.

Do I need to be a developer to use Cask Hydrator?
-------------------------------------------------
It depends; if you are building data pipelines using the available plugins, then no. But
if you want to implement a custom plugin, then you need to have knowledge of Java and the
relevant CDAP APIs.

Is Cask Hydrator available in CDAP standalone mode?
---------------------------------------------------
It's available as part of CDAP Standalone, and included in the CDAP SDK.

On which public clouds does CDAP work?
--------------------------------------
Customers have CDAP running in AWS and GCE, with support for running on Azure forthcoming.

Does Cask Hydrator integrate with Apache Sentry for authorization?
------------------------------------------------------------------
Cask Hydrator is built using CDAP. The current stable version of CDAP is not yet
integrated with Sentry for authorization. Active work is being done on integrating CDAP
with Apache Sentry. More information about the design can be found here. The first phase
of Sentry integration is available in CDAP 3.4. This initial phase includes integration of
authorization for runtime components. CDAP 3.5 will include authorization for Datasets.
 
If I have a question about a data pipeline I am building, how can I get help?
-----------------------------------------------------------------------------
Cask Hydrator and CDAP have an open source support system. It has a Google group where
developers and users can ask questions, available at cdap-users@googlegroups.com. Cask
offers commercial CDAP subscriptions, with additional support options.
 
If I have found a issue with Cask Hydrator, how can I report it?
----------------------------------------------------------------
You can use the open source JIRA system for CDAP, Cask Hydrator, and other projects. If
you have a commercial CDAP subscription, the Cask support portal is available for
customers to report issues.
 
I am interested in contributing to Cask Hydrator; how can I get started?
------------------------------------------------------------------------
Cask Hydrator is an open source project licensed under the Apache 2.0 license, though not
currently part of the Apache Foundation. In order to contribute to Cask Hydrator, you may
need to provide to Cask a signed ICLA or CCLA. The terms are open and very similar to the
Apache Foundation. Contact Cask through the cdap-users@googlegroups.com for more
information. 
 
I have a feature request for Cask Hydrator; how can I get it added?
-------------------------------------------------------------------
You can file a JIRA ticket for the feature request or, if you have a commercial CDAP
subscription, you can use the support portal.
