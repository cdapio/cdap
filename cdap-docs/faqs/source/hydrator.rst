.. meta::
    :author: Cask Data, Inc.
    :description: Frequently Asked Questions about starting the Cask Data Application Platform
    :copyright: Copyright © 2015 Cask Data, Inc.

:titles-only-global-toc: true

.. _faqs-cdap:

===============
FAQs: HYDRATOR
===============

What should I do when I see the following errors in User Interface?
-------------------------------------------------------------------

Error parsing widgets JSON for the plugin
.........................................

This error means the widget JSON for the particular plugin has error in its JSON. The most likely case why this error happens could be because one of the following reasons,

  - You are missing a comma (',') in your JSON
  - You have a missing double quotes ('"') in you JSON
  - You have value for key that is not a valid JSON value (for instance, NaN)

The fastest way to fix this issue is to run the plugin's Widget JSON through a JSON linter to check if it is a valid JSON.

No widgets JSON found for the plugin
....................................

This error means the backend could not find the widget JSON for the plugin that you are currently working with. As part of plugin deployment, a JAR and a JSON file are deployed with a plugin. <REFERENCE to WIDGET-JSON DOCUMENTATION SECTION>

If I have Widget JSON, what is the order in which plugin properties appear in UI?
----------------------------------------------------------------------------------

The properties are specified as a list inside the configuration groups. So the properties of the plugin will appear inside the group in the same order as they are listed.

What happens if I don't have a property of the plugin in Widget JSON?
---------------------------------------------------------------------

The Hydrator UI will create a separate group named 'Generic' and adds all properties as part of that particular group and defaults all properties to textbox widget.

What happens when I use an invalid widget (non-existant) in Widget JSON for a particular property in a
------------------------------------------------------------------------------------------------------ plugin
------

In the case when Widget JSON has a non-existant widget the Hydrator UI defaults to a textbox field.
