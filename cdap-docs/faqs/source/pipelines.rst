.. meta::
    :author: Cask Data, Inc.
    :description: Frequently Asked Questions about Cask Data Application Platform Pipelines
    :copyright: Copyright © 2016-2017 Cask Data, Inc.

:titles-only-global-toc: true

.. _faqs-cdap-pipelines:

====================
FAQs: CDAP Pipelines
====================

What should I do if I see any of these errors in the CDAP Pipeline Studio User Interface?
----------------------------------------------------------------------------------
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

In which order do plugin properties appear in the CDAP Pipeline Studio?
-----------------------------------------------------------------------
The properties are specified as a list inside the configuration groups. Properties of the plugin
will appear inside the group in the same order as they are listed.

What happens if I don't have a property of the plugin in the Widget JSON?
-------------------------------------------------------------------------
The CDAP Pipeline Studio UI will create a separate group named 'Generic', add all properties as 
part of that group, and (by default) display all properties in a textbox widget.

What happens when I use an invalid widget in the Widget JSON?
-------------------------------------------------------------
In a case where the Widget JSON includes a non-existent (or unknown) widget, 
the CDAP Pipeline Studio UI defaults to a textbox field.
