.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2016 Cask Data, Inc.

.. _cask-hydrator-packaging-plugins:

=================
Packaging Plugins
=================

.. NOTE: Because of the included files in this file, use these levels for headlines/titles:
  .. 1 -----
  .. 2 .....
  .. 3 `````
  .. etc.

To **package, present,** and **deploy** your plugin, see these instructions:

- :ref:`Plugin Packaging: <cask-hydrator-packaging-plugins-packaging>` packaging in a JAR
- :ref:`Plugin Presentation: <cask-hydrator-packaging-plugins-presentation>` controlling
  how your plugin appears in the Hydrator Studio

If you are installing a **third-party JAR** (such as a **JDBC driver**) to make it accessible to other
plugins or applications, see :ref:`these instructions <cask-hydrator-plugin-management-third-party-plugins>`.

..   - UI
..   - REST
..   - CLI 

.. Plugin Packaging
.. ----------------
.. _cask-hydrator-packaging-plugins-packaging:

.. include:: /../../developers-manual/source/building-blocks/plugins.rst
   :start-after: .. _plugins-deployment-packaging:
   :end-before:  .. _plugins-deployment-system:
   
By using one of the available :ref:`Maven archetypes
<cask-hydrator-developing-plugin-basics-maven-archetypes>`, your project will be set up to
generate the required JAR manifest. If you move the plugin class to a different Java
package after the project is created, you will need to modify the configuration of the
``maven-bundle-plugin`` in the ``pom.xml`` file to reflect the package name changes.

If you are developing plugins for the ``cdap-data-pipeline`` artifact, be aware that for
classes inside the plugin JAR that you have added to the Hadoop Job configuration directly
(for example, your custom ``InputFormat`` class), you will need to add the Java packages
of those classes to the "Export-Package" as well. This is to ensure those classes are
visible to the Hadoop MapReduce framework during the plugin execution. Otherwise, the
execution will typically fail with a ``ClassNotFoundException``.


.. _cask-hydrator-packaging-plugins-presentation:

Plugin Presentation
-------------------
When a plugin is displayed in the CDAP UI, its properties are represented by widgets in
the Cask Hydrator Studio. Each property of a plugin is represented by default as a
textbox in the user interface. By including an appropriate plugin widget JSON file, you
can customize that presentation to use a different interface, if desired.

.. _cask-hydrator-creating-custom-plugins-widget-json:

Plugin Widget JSON
------------------
To customize the plugin display, a plugin can include a widget JSON file that specifies
the particular widgets and sets of widget attributes used to display the plugin properties
in the CDAP UI.

The widget JSON is composed of two lists:

- a list of property configuration groups; and
- a list of output properties.

.. highlight:: json-ellipsis  

For example::

  {
    "configuration-groups": [
      {"group-1"},
      {"group-2"},
      ...
    ],
    "outputs": [
      {"ouput-property-1"},
      {"ouput-property-2"},
    ]
  }

.. highlight:: json

Configuration Groups
....................
Configuration groups are a simple grouping of properties of a plugin. A configuration
group is represented as a JSON object with a label and a list of plugin properties for that
group.

For example, in a *Batch Source* plugin, properties such as *Dataset Name*, *Dataset Base
Path*, *Duration*, and *Delay* can be grouped as the *Batch Source Configuration*.

In the case of the *Batch Source* plugin, it could look like this::

  {
    "configuration-groups": [
      {
        "label": "Batch Source Configuration",
        "properties": [
          {"field1"},
          {"field2"},
          {"field3"}
        ]
      }
    ],
    "outputs": [
      {"output-property1"},
      {"output-property2"}
    ]
  }

Once a group is established, we can configure how each of the properties inside the group is
represented in the CDAP UI.

The configuration of each property of the plugin is composed of:

- :ref:`widget-type: <cask-hydrator-creating-custom-plugins-custom-widgets>` The type of
  widget needed to represent this property.
- **label:** Label to be used in the CDAP UI for the property.
- **name:** Name of the field (as supplied by the CDAP UI backend).
- **widget-attributes:** A map of attributes that the widget type requires be defined in
  order to render the property in the CDAP UI. The attributes vary depending on the
  widget type.
- :ref:`plugin-function: <cask-hydrator-creating-custom-plugins-custom-plugin-function>`
  An optional map of plugin method and its widget attributes that can be applied to a
  particular plugin property.

Note that with the exception of the value of the *label*, all property values are
case-sensitive.

To find the available field names, you can use the Artifact HTTP RESTful API to
:ref:`retrieve plugin details <http-restful-api-artifact-plugin-detail>` for an artifact,
which will include all the available names. (If the artifact is your own, you will already
know the available field names from your source code.)

In the case of our *Batch Source* plugin example, the ``configuration-groups`` can be
represented by::

  {
    "configuration-groups": [
      {
        "label": "Batch Source",
        "properties": [
          {
            "widget-type": "dataset-selector",
            "label": "Dataset Name",
            "name": "name"
          },
          {
            "widget-type": "textbox",
            "label": "Dataset Base Path",
            "name": "basePath"
          },
          {
            "widget-type": "textbox",
            "label": "Group By Fields",
            "name": "groupByFields",
            "plugin-function": {
              "method": "POST",
              "widget": "outputSchema",
              "output-property": "schema",
              "plugin-method": "outputSchema",
              "required-fields": ["groupByFields", "aggregates"],
              "missing-required-fields-message":
                "'Group By Fields' & 'Aggregates' properties are required to fetch schema."
            }
          },
          {
            "widget-type": "textbox",
            "label": "Delay",
            "name": "delay"
          }
        ]
      }
    ]
  }
  
.. _cask-hydrator-creating-custom-plugins-custom-widgets:

Widgets
.......

A widget in the CDAP UI represents a component that will be rendered and used to set a
value of a property of a plugin. These are the different widgets |---| their type, a
description, their attributes (if any), and their output data type |---| that we support in
Cask Hydrator as of version |version|:

.. list-table::
   :widths: 20 25 25 25
   :header-rows: 1

   * - Widget Type
     - Description
     - Attributes
     - Output Data Type
   * - ``textbox``
     - Default HTML textbox, used to enter any string
     - ``default``: default value for the widget
     - ``string``
   * - ``number``
     - Default HTML number textbox that only accepts valid numbers
     - | ``default``: default value for the widget
       | ``min``: minimum value for the number box
       | ``max``: maximum value for the number box
     - ``string``
   * - ``passwordbox``
     - Default HTML password entry box
     - No attributes
     - ``string``
   * - ``csv``
     - Comma-separated values; each value is entered in a separate box
     - No attributes
     - Comma-separated ``string``
   * - ``dsv``
     - Delimiter-separated values; each value is entered in a separate box
     - ``delimiter``: delimiter used to separate the values
     - Delimiter-separated ``string``
   * - ``json-editor``
     - JSON editor that pretty-prints and auto-formats JSON while it is being entered
     - ``default``: default serialized JSON value for the widget
     - ``string``
   * - ``javascript-editor``, ``python-editor``
     - An editor to write JavaScript (``javascript-editor``) or Python (``python-editor``)
       code as a value for a property
     - ``default``: default ``string`` value for the widget
     - ``string``
   * - ``keyvalue``
     - A key-value editor for constructing maps of key-value pairs
     - | ``delimiter``: delimiter for the key-value pairs
       | ``kv-delimiter``: delimiter between key and value
     - ``string``
   * - ``keyvalue-dropdown``
     - Similar to *keyvalue* widget, but with a drop-down value list
     - | ``delimiter``: delimiter for the key-value pairs
       | ``kv-delimiter``: delimiter between key and value
       | ``dropdownOptions``: list of drop-down options to display
     - ``string``
   * - ``select``
     - An HTML drop-down with a list of values; allows one choice from the list
     - | ``values``: list of values for the drop-down
       | ``default``: default value from the list
     - ``string``
   * - ``dataset-selector``, ``stream-selector``
     - A type-ahead textbox with a list of datasets (``dataset-selector``) or streams
       (``stream-selector``) from the CDAP instance
     - No attributes
     - ``string``
   * - ``schema``
     - A four-column, editable table to represent a schema of a plugin
     - | ``schema-types``: list of schema types for each field from which the user can chose when setting the schema
       | ``schema-default-type``: default type for each newly-added field in the schema
     - ``string``
   * - ``non-editable-schema-editor``
     - A non-editable widget for displaying a schema
     - ``schema``: schema that will be used as the output schema for the plugin
     - ``string``
   * - ``ds-multiplevalues``
     - A delimiter-separated values widget that allows specifying lists of values separated by delimiters
     - | ``numValues``: number of values (number of delimiter-separated values)
       | ``values-delimiter``: the delimiter between each value
       | ``delimiter``: the delimiter between each *set* of values
       | ``placeholders``: array of placeholders for each value's textbox
     - ``string``

.. _cask-hydrator-creating-custom-plugins-custom-plugin-function:

Plugin Functions
................
Plugin functions are methods exposed by a particular plugin that can be used to fetch
output schema for a plugin. These are the fields that need  to be configured to use the
plugin functions in the CDAP UI:

- **method:** Type of request to make when calling the plugin function from the CDAP UI
  (for instance GET or POST).
- **widget:** Type of widget to use to import output schema.
- **output-property:** Property to update once the CDAP UI receives the data from the plugin method.
- **plugin-method:** Name of the plugin method to call (as exposed by the plugin).
- **required-fields:** Fields required to call the plugin method.
- **missing-required-fields-message:** A message for the user as to why the action is
  disabled in the CDAP UI, when the required fields are missing values.

The last two properties are solely for the the CDAP UI and are not required all the time.
However, the first four fields are required fields to use a plugin method in the CDAP UI.
In the case of a plugin function, if the widget is not supported in the CDAP UI or the
plugin function map is not supplied, the user will not see it in the CDAP UI.

Outputs
.......
The *outputs* is a list of plugin properties that represent the output schema of a particular plugin.

The output schema for a plugin can be represented in two different ways, either:

- via an *implicit* schema; or
- via an *explicit* ``Schema`` property.

An **implicit** schema is a pre-determined output schema for a plugin that the plugin developer
enforces. The implicit schema is not associated with any properties of the plugin, but
just enforces the output schema of the plugin, for visual display purposes. An example of
this is the `Twitter real-time source plugin
<https://github.com/caskdata/hydrator-plugins/blob/release/1.2/core-plugins/widgets/Twitter-realtimesource.json>`__.

An **explicit** ``Schema`` property is one that can be defined as the output schema and
can be appropriately configured to make it editable through the CDAP UI.

Output properties are configured in a similar manner as individual properties in
configuration groups. They are composed of a name and a widget type, one of either
``schema`` or ``non-editable-schema-editor``.

With the ``schema`` widget type, a list of widget attributes can be included; with
``non-editable-schema-editor``, a schema is added instead.

For example:

Our "Batch Source" plugin could have a configurable output schema::

  {
    "outputs": [
      {
        "name": "schema",
        "widget-type": "schema",
        "widget-attributes": {
          "schema-types": [
            "boolean",
            "int",
            "long",
            "float",
            "double",
            "string",
            "map<string, string>"
          ],
          "schema-default-type": "string"
        }
      }
    ]
  }

The output properties of the Twitter real-time source, with an explicit, non-editable schema property,
composed of the fields *id*, *time*, *favCount*, *rtCount*, *geoLat*, *geoLong*, and *isRetweet*::

  {
    "outputs": [
      {
        "widget-type": "non-editable-schema-editor",
        "schema": {
          "id": "long",
          "message": "string",
          "lang": [
            "string",
            "null"
          ],
          "time": [
            "long",
            "null"
          ],
          "favCount": "int",
          "rtCount": "int",
          "source": [
            "string",
            "null"
          ],
          "geoLat": [
            "double",
            "null"
          ],
          "geoLong": [
            "double",
            "null"
          ],
          "isRetweet": "boolean"
        }
      }
    ]
  }

Widget types for output properties are limited to ensure that the schema that is propagated across
different plugins in the CDAP UI is consistent.

Example Widget JSON
...................
Based on the above definitions, we could write the complete widget JSON for our *Batch Source* plugin
(with the properties of *name*, *basePath*, *duration*, *delay*, and an output *schema*) as::

  {
    "metadata": {
      "spec-version": "1.0"
    },
    "configuration-groups": [
      {
        "label": "Batch Source",
        "properties": [
          {
            "widget-type": "dataset-selector",
            "label": "Dataset Name",
            "name": "name"
          },
          {
            "widget-type": "textbox",
            "label": "Dataset Base Path",
            "name": "basePath"
          },
          {
            "widget-type": "textbox",
            "Label": "Group By Fields",
            "name": "groupByFields",
            "plugin-function": {
              "method": "POST",
              "widget": "outputSchema",
              "output-property": "schema",
              "plugin-method": "outputSchema",
              "required-fields": ["groupByFields", "aggregates"],
              "missing-required-fields-message":
                "'Group By Fields' & 'Aggregates' properties are required to fetch schema."
            }
          },
          {
            "widget-type": "textbox",
            "label": "Delay",
            "name": "delay"
          }
        ]
      }
    ],
    "outputs": [
      {
        "name": "schema",
        "widget-type": "schema",
        "widget-attributes": {
          "schema-types": [
            "boolean",
            "int",
            "long",
            "float",
            "double",
            "string",
            "map<string, string>"
          ],
          "schema-default-type": "string"
        }
      }
    ]
  }
  
