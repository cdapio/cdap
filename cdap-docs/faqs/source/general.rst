.. meta::
    :author: Cask Data, Inc.
    :description: Frequently Asked Questions about the Cask Data Application Platform
    :copyright: Copyright © 2014-2016 Cask Data, Inc.

:titles-only-global-toc: true

.. _faqs-general-questions:

=======================
FAQs: General Questions
=======================

What is available in the CDAP SDK?
----------------------------------
The CDAP SDK includes:

- Local instance of the CDAP Server;
- CDAP APIs for building applications;
- CDAP UI and CDAP CLI for interacting with CDAP; and
- Examples for getting started with CDAP.


What platforms are supported by the Cask Data Application Platform SDK?
-----------------------------------------------------------------------
The CDAP SDK has been extensively tested on Mac OS X and Linux. CDAP on Windows has not
been extensively tested. If you have any issues with CDAP on Windows, help us by 
`filing a ticket <https://issues.cask.co/browse/CDAP>`__.

What programming languages are supported by CDAP?
-------------------------------------------------
CDAP currently supports Java for developing applications.

What version of Java SDK is required by CDAP?
---------------------------------------------
The latest version of the JDK or JRE version 7 or version 8 must be installed in your
environment. CDAP is tested on both the `Oracle JDK <http://www.java.com/en/download/manual.jsp>`__ 
and the `OpenJDK <http://openjdk.java.net/>`__.

What version of Node.js is required by CDAP?
--------------------------------------------
We recommend a version of `Node.js <https://nodejs.org/>`__ beginning with |node-js-min-version|.

.. We support Node.js up to |node-js-max-version|.

I have a Hadoop cluster in my data center, can I run CDAP that uses my Hadoop cluster?
--------------------------------------------------------------------------------------
Yes. You can install Distributed CDAP on your Hadoop cluster. See the :ref:`Installation procedure <installation-index>`.

What Hadoop distributions can CDAP run on?
------------------------------------------
CDAP |version| has been tested on and supports CDH |cdh-versions|, HDP |hdp-versions|, 
MapR |mapr-versions|, Amazon Hadoop (EMR) |emr-versions|, and Apache Bigtop 1.0. 

I'm seeing log messages about the failure to send audit log message while running the SDK. What's wrong?
--------------------------------------------------------------------------------------------------------
If your computer goes into sleep mode, then you may see these messages after the machine
is restored from sleep mode, as the audit logs are not getting delivered while the machine
sleeps. You may need to restart CDAP to restore its state depending on how long it has
been asleep.


.. _faq-cdap-user-groups:

I've found a bug in CDAP. How do I file an issue?
-------------------------------------------------
You can use the `CDAP User GoogleGroup <https://groups.google.com/d/forum/cdap-user>`__ to
report issues or file a ticket using the available `CDAP JIRA system
<https://issues.cask.co/browse/CDAP>`__.

What User Groups and Mailing Lists are available about CDAP?
------------------------------------------------------------
- `cdap-user@googlegroups.com <https://groups.google.com/d/forum/cdap-user>`__

The *cdap-user* mailing list is primarily for users using the product to develop
applications. You can expect questions from users, release announcements, and any other
discussions that we think will be helpful to the users.

- `cdap-dev@googlegroups.com <https://groups.google.com/d/forum/cdap-dev>`__

The *cdap-dev* mailing list is essentially for developers actively working
on the product, and should be used for all our design, architecture and technical
discussions moving forward. This mailing list will also receive all JIRA and GitHub
notifications.

Is CDAP on IRC?
---------------
**CDAP IRC Channel:** #cdap on `chat.freenode.net <irc://chat.freenode.net:6667/cdap>`__.






