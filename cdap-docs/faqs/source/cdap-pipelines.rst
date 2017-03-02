.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2016-2017 Cask Data, Inc.
    :description: FAQ, Frequently Asked Questions and terms related to CDAP Pipelines, ETL, and Data Pipelines

:titles-only-global-toc: true

.. _cdap-pipelines-faqs:

====================
FAQs: CDAP Pipelines
====================

.. contents:: 
    :local:
    :backlinks: none


Do I need to be a developer to use CDAP Pipelines?
--------------------------------------------------
It depends; if you are building data pipelines using the available plugins, then no. But
if you want to implement a custom plugin, then you need to have knowledge of Java and the
relevant CDAP APIs.

Are CDAP pipelines available in Standalone CDAP mode?
-----------------------------------------------------
It's part of the Standalone CDAP, included in the CDAP SDK.

On which public clouds does CDAP work?
--------------------------------------
Customers have CDAP running in AWS and GCE, with support for running on Azure forthcoming.

Do CDAP Pipelines integrate with Apache Sentry for authorization?
-----------------------------------------------------------------
CDAP supports :ref:`integration with Sentry <apache-sentry>` for authorization. Since CDAP
pipelines are built using CDAP, the :ref:`same authorization policies
<admin-authorization>` can and are applied as in CDAP.

If I have a question about a data pipeline I am building, how can I get help?
-----------------------------------------------------------------------------
CDAP has an open source support system. It has a Google group where developers and users
can ask questions, available at cdap-users@googlegroups.com. Cask offers commercial CDAP
subscriptions, with additional support options.
 
If I have found a issue with CDAP pipelines, how can I report it?
-----------------------------------------------------------------
You can use the open source JIRA system for CDAP, CDAP pipelines, and other projects. If
you have a commercial CDAP subscription, the Cask support portal is available for
customers to report issues.
 
I am interested in contributing to CDAP Pipelines; how can I get started?
-------------------------------------------------------------------------
CDAP is an open source project licensed under the Apache 2.0 license, though not currently
part of the Apache Foundation. In order to contribute to CDAP, you may need to provide to
Cask a signed ICLA or CCLA. The terms are open and very similar to the Apache Foundation.
Contact Cask through the cdap-users@googlegroups.com for more information. 
 
I have a feature request for CDAP Pipelines; how can I get it added?
--------------------------------------------------------------------
You can file a JIRA ticket for the feature request or, if you have a commercial CDAP
subscription, you can use the support portal.

What should I do if I see any of these errors in the CDAP Studio User Interface?
--------------------------------------------------------------------------------
- **Error parsing widgets JSON for the plugin <plugin-name>**

  This error means the :ref:`widget JSON file <cdap-pipelines-creating-custom-plugins-widget-json>` 
  for the particular plugin has an error in its JSON, such as:

  - You are missing a comma character (``,``) in the JSON
  - You have a missing a double quote character (``"``) in the JSON
  - You have a value for a key that is not a valid JSON value (for instance, ``NaN``)

  One way to fix these issues is to use ``json-lint`` to identify problems and make sure the widget JSON is valid.

- **No widgets JSON found for the plugin <plugin-name>**

  This error means that the CDAP UI Service could not find the widget JSON for the plugin that you are
  currently working with. As part of plugin deployment, a JAR and a JSON file are deployed with a plugin,
  as described in the :ref:`cdap-pipelines-packaging-plugins`.

In which order do plugin properties appear in the CDAP Studio?
--------------------------------------------------------------
The properties are specified as a list inside the configuration groups. Properties of the plugin
will appear inside the group in the same order as they are listed.

What happens if I don't have a property of the plugin in the Widget JSON?
-------------------------------------------------------------------------
The CDAP Studio UI will create a separate group named 'Generic', add all properties as 
part of that group, and (by default) display all properties in a textbox widget.

What happens when I use an invalid widget in the Widget JSON?
-------------------------------------------------------------
In a case where the Widget JSON includes a non-existent (or unknown) widget, 
the CDAP Studio UI defaults to a textbox field.

My plugin is not showing up correctly in the CDAP Studio; what should I look at?
--------------------------------------------------------------------------------
If you are not seeing the correct widget or the correct default value set in your widget's
JSON file, check all the spelling of properties and their values in the widget JSON file.

In particular, check that all values of the properties ``widget-type`` and
``widget-attributes`` are spelled correctly. As these values are case-sensitive, errors
can easily be made that can be hard to uncover in the UI itself.
