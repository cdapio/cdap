.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2016-2017 Cask Data, Inc.

.. _plugins-presentation:
.. _cdap-pipelines-packaging-plugins-presentation:
.. _cdap-pipelines-creating-custom-plugins-widget-json:

===================
Plugin Presentation
===================

When a plugin is displayed in the CDAP UI, its properties are represented by widgets in
the CDAP Studio. Each property of a plugin is represented, by default, as a simple
textbox in the user interface. By including an appropriate plugin widget JSON file, you
can customize that presentation to use a different interface, as desired.

The available widgets that can be used are :ref:`described below
<plugins-presentation-widgets>`. Note that some of the plugins available for CDAP may
include use of additional widgets that are not listed here. We recommend only using the
list of plugins shown here, as these other widgets are internal to CDAP and may not be
supported in a future release.

This document describes version |plugins-spec-version| of the plugin specification.
Changes to the specification are described in the  
:ref:`plugins-presentation-specification-changes` and should be checked if you are using a 
version of the specification earlier than the current.


.. _plugins-presentation-widget-json:

Plugin Widget JSON
==================
To customize the plugin display, a plugin can include a widget JSON file that specifies
the particular widgets and the sets of widget attributes used to display the plugin properties
in the CDAP UI.

The widget JSON is composed of:

- a map of :ref:`metadata <plugins-presentation-metadata>`
- a list of property :ref:`configuration groups <plugins-presentation-configuration-groups>`
- a map of :ref:`inputs <plugins-presentation-inputs>`
- a list of :ref:`outputs <plugins-presentation-outputs>`
- a map of :ref:`stream or dataset jumps <plugins-presentation-jumps>`

.. highlight:: json-ellipsis

Each configuration group consists of a list of the :ref:`individual properties
<plugins-presentation-property-configuration>` and the 
:ref:`widgets <plugins-presentation-widgets>` to be used. For example::

  {
    "metadata": {
      ...
    },
    "configuration-groups": [
      {"label": "Group 1",
        "properties": [
        ...
        ]
      },
      {"label": "Group 2",
        "properties": [
        ...
        ]
      },
      ...
    ],
    "inputs": {
      ...
    },
    "outputs": [
      {"output-property-1"},
      {"output-property-2"},
      ...
    ],
    "jump-config": {
      ...
    }
  }

.. _plugins-presentation-metadata:

Metadata
--------
Metadata refers to the top-level information about a plugin. The only information required is the
a *map* consisting of the ``spec-version``, the version of the specification which the JSON follows. 

Current version: |plugins-spec-version|. For example:

.. parsed-literal::

  {
    "metadata": {
      "spec-version" : "|plugins-spec-version|"
    },
    ...
  }

.. _plugins-presentation-configuration-groups:

Configuration Groups
--------------------
Configuration groups are a simple grouping of properties of a plugin. A configuration
group is represented as a JSON object with a *label* and an ordered *list* of plugin
properties for that group.

For example, in a *Batch Source* plugin, properties such as ``name``, ``basePath``,
``duration``, and ``delay`` could be grouped into a *Batch Source Configuration*.

.. highlight:: json-ellipsis

In the case of a *Batch Source* plugin, it could look like this::

  {
    "configuration-groups": [
      {
        "label": "Batch Source Configuration",
        "properties": [
          {
            "name": "name",
            ...
          },
          {
            "name": "basePath",
            ...
          },
          {
            "name": "duration",
            ...
          },
          {
            "name": "delay",
            ...
          }
        ]
      }
    ]
    ...
  }

Once a group is established, you can configure how each of the individual properties
inside the group is represented in the CDAP UI.

.. _plugins-presentation-property-configuration:

Property Configuration
----------------------
Each individual property of the plugin is represented by a configuration, composed of:

- **name:** Name of the field (as supplied by the CDAP server for the artifact).
- **label:** Text string displayed in the CDAP UI beside the widget.
- :ref:`widget-type: <plugins-presentation-widgets>` The type of
  widget to be used to represent this property.
- :ref:`widget-attributes: <plugins-presentation-widgets>` A map of attributes that the
  widget type requires to be defined in order to render the property in the CDAP UI. The
  attributes required depend on the widget type used.
- :ref:`plugin-function: <plugins-presentation-plugin-function>`
  An optional map of plugin method and its widget attributes that can be applied to a
  particular plugin property.

Note that all properties and property values are case-sensitive.

To find the available field names, you can use the :ref:`Artifact HTTP RESTful API 
<http-restful-api-artifact>` to :ref:`retrieve plugin details 
<http-restful-api-artifact-plugin-detail>` for an artifact, which will include all the
available field names. (If the artifact is your own, you will already know the available
field names from your source code.)

In this example of a *Batch Source* plugin and its ``configuration-groups``, four
different properties are defined; three use a *textbox* widget, while one uses a
*dataset-selector*. The *groupByFields* property includes a :ref:`plugin function
<plugins-presentation-plugin-function>`, used to fetch an output schema for the widget::

  {
    "configuration-groups": [
      {
        "label": "Batch Source",
        "properties": [
          {
            "name": "name",
            "label": "Dataset",
            "widget-type": "dataset-selector"
          },
          {
            "name": "basePath",
            "label": "Base Path",
            "widget-type": "textbox"
          },
          {
            "name": "groupByFields",
            "label": "Group By Fields",
            "widget-type": "textbox",
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
            "name": "duration",
            "widget-type": "textbox"
          },
          ...
        ]
      }
    ]
  }

.. _plugins-presentation-widgets:

Plugin Widgets
==============
A widget in the CDAP UI represents a component that will be rendered and used to set the
value of a property of a plugin. These are the different widgets |---| their type, their
attributes (if any), their output data type, a description, sample JSON |---| that we support in
CDAP pipelines as of version |version|.

.. highlight:: json-ellipsis

.. list-table::
   :widths: 15 20 15 20 30
   :header-rows: 1

   * - Widget Type
     - Widget Attributes
     - Output Data Type
     - Description
     - Example Widget JSON
     
   * - ``csv``
     - No attributes
     - Comma-separated ``string``
     - Comma-separated values; each value is entered in a separate box
     - .. container:: copyable copyable-text
     
         ::

          {
            "name": "property-csv",
            "widget-type": "csv",
            "widget-attributes": {}
          }

   * - ``dataset-selector``
     - No attributes
     - ``string``
     - A type-ahead textbox with a list of datasets from the CDAP instance
     - .. container:: copyable copyable-text
     
         ::

          {
            "name": "property-dataset-selector",
            "widget-type": "dataset-selector",
            "widget-attributes": {}
          }

   * - ``ds-multiplevalues``
     - - ``delimiter``: the delimiter between each *set* of values
       - ``numValues``: number of values (number of delimiter-separated values)
       - ``placeholders``: array of placeholders for each value's textbox
       - ``values-delimiter``: the delimiter between each value
     - ``string``
     - A delimiter-separated values widget that allows specifying lists of values
       separated by delimiters
     - .. container:: copyable copyable-text
     
         ::

          {
            "name": "property-ds-multiplevalues",
            "widget-type": "ds-multiplevalues",
            "widget-attributes": {
              "delimiter": ",",
              "values-delimiter": ":",
              "numValues": "3",
              "placeholders": [
                "Input Field", 
                "Lookup", 
                "Output Field"
              ]
            }
          }

   * - ``dsv``
     - ``delimiter``: delimiter used to separate the values
     - Delimiter-separated ``string``
     - Delimiter-separated values; each value is entered in a separate box
     - .. container:: copyable copyable-text
     
         ::

          {
            "name": "property-dsv",
            "widget-type": "dsv",
            "widget-attributes": {
              "delimiter": ":"
            }
          }
     
   * - ``hidden``
     - ``default``: default ``string`` value for the widget
     - ``string``
     - This "hidden" widget allows values to be set for a property but hidden from users.
       A default can be supplied that will be used as the value for the property.
     - .. container:: copyable copyable-text

         ::

          {
            "name": "property-hidden",
            "widget-type": "hidden",
            "widget-attributes": {
              "default": "defaultValue"
            }
          }

   * - ``input-field-selector``
     - No attributes
     - ``string``
     - A dropdown widget with a list of columns taken from the input schema. 
       Selecting sets the input column for that plugin property.
     - .. container:: copyable copyable-text
     
         ::

          {
            "name": "Property1",
            "widget-type": "csv",
            "widget-attributes": {}
          }
     
   * - ``javascript-editor``
     - ``default``: default ``string`` value for the widget
     - ``string``
     - An editor to write JavaScript code as a value of a property
     - .. container:: copyable copyable-text
     
         ::

          {
            "name": "property-javascript-editor",
            "widget-type": "javascript-editor",
            "widget-attributes": {
              "default": 
                "function transform(input, emitter, context) {\
          \\n  emitter.emit(input);\\n}"
            }
          }

   * - ``json-editor``
     - ``default``: default serialized JSON value for the widget
     - ``string``
     - A JSON editor that pretty-prints and auto-formats JSON while it is being entered
     - .. container:: copyable copyable-text
     
         ::

          {
            "name": "property-json-editor",
            "widget-type": "json-editor",
            "widget-attributes": {
              "default": "{ \"p1\": \"value\" }"
            }
          }
     
   * - ``keyvalue``
     - - ``delimiter``: delimiter for the key-value pairs
       - ``kv-delimiter``: delimiter between key and value
     - ``string``
     - A key-value editor for constructing maps of key-value pairs
     - .. container:: copyable copyable-text
     
         ::

          {
            "name": "property-keyvalue",
            "widget-type": "keyvalue",
            "widget-attributes": {
                "delimiter": ",",
                "kv-delimiter": ":"
            }
          }
     
   * - ``keyvalue-dropdown``
     - - ``delimiter``: delimiter for the key-value pairs
       - ``dropdownOptions``: list of drop-down options to display
       - ``kv-delimiter``: delimiter between key and value
     - ``string``
     - Similar to *keyvalue* widget, but with a drop-down value list
     - .. container:: copyable copyable-text
     
         ::

          {
            "name": "property-keyvalue-dropdown",
            "widget-type": "keyvalue-dropdown",
            "widget-attributes": {
                "delimiter": ",",
                "kv-delimiter": ":",
                "dropdownOptions": [ "Option1", "Option2"]
            }
          }
     
   * - ``non-editable-schema-editor``
     - ``schema``: schema that will be used as the output schema for the plugin
     - ``string``
     - A non-editable widget for displaying a schema
     - .. container:: copyable copyable-text
     
         ::

          {
            "name": "property-non-editable-schema-editor",
            "widget-type": "non-editable-schema-editor",
            "widget-attributes": {}
          }
     
   * - ``number``
     - - ``default``: default value for the widget
       - ``max``: maximum value for the number box
       - ``min``: minimum value for the number box
     - ``string``
     - Default HTML number textbox that only accepts valid numbers
     - .. container:: copyable copyable-text
     
         ::

          {
            "name": "property-number",
            "widget-type": "number",
            "widget-attributes": {
              "default": "1",
              "min": "1",
              "max": "100"
            }
          }
     
   * - ``password``
     - No attributes
     - ``string``
     - Default HTML password entry box
     - .. container:: copyable copyable-text
     
         ::

          {
            "name": "property-password",
            "widget-type": "password",
            "widget-attributes": {}
          }
     
   * - ``python-editor``
     - ``default``: default ``string`` value for the widget
     - ``string``
     - An editor to write Python code as a value of a property
     - .. container:: copyable copyable-text
     
         ::

          {
            "name": "property-python-editor",
            "widget-type": "python-editor",
            "widget-attributes": {
              "default": 
                "def transform(input, emitter, context):\
          \\n  emitter.emit(input)\\n"
            }
          }
     
   * - ``schema``
     - - ``schema-default-type``: default type for each newly-added field in the schema
       - ``schema-types``: list of schema types for each field from which the user can chose when setting the schema
     - ``string``
     - A four-column, editable table for representing the schema of a plugin
     - .. container:: copyable copyable-text
     
         ::

          {
            "name": "property-schema",
            "widget-type": "schema",
            "widget-attributes": {
              "schema-default-type": "string",
              "schema-types": [
                "boolean",
                "int",
                "long",
                "float",
                "double",
                "bytes",
                "string",
                "map<string, string>"
              ]
            }
          }
     
   * - ``select``
     - - ``default``: default value from the list
       - ``values``: list of values for the drop-down
     - ``string``
     - An HTML drop-down with a list of values; allows one choice from the list
     - .. container:: copyable copyable-text
     
         ::

          {
            "name": "property-select",
            "widget-type": "select",
            "widget-attributes": {
                "default": "Bananas",
                "values": ["Apples", "Oranges", "Bananas"]
            }
          }
     
   * -  ``stream-selector``
     - No attributes
     - ``string``
     - A type-ahead textbox with a list of streams from the CDAP instance
     - .. container:: copyable copyable-text
     
         ::

          {
            "name": "property-stream-selector",
            "widget-type": "stream-selector",
            "widget-attributes": {}
          }

   * - ``textarea``
     - - ``default``: default value for the widget
       - ``rows``: height of the ``textarea``
     - ``string``
     - An HTML ``textarea`` element which accepts a default value attribute and a height in rows
     - .. container:: copyable copyable-text
     
         ::

          {
            "name": "property-textarea",
            "widget-type": "textarea",
            "widget-attributes": {
              "default": "Default text.",
              "rows": "1"
            }
          }
     
   * - ``textbox``
     - ``default``: default value for the widget
     - ``string``
     - An HTML textbox, used to enter any string, with a default value attribute
     - .. container:: copyable copyable-text
     
         ::

          {
            "name": "property-textbox",
            "widget-type": "textbox",
            "widget-attributes": {
              "default": "Default text."
            }
          }


.. _plugins-presentation-plugin-function:

Plugin Function
---------------
A plugin function is a method exposed by a particular plugin that can be used for a
specific task, such as fetching an output schema for a plugin. 

These fields need to be configured to use the plugin functions in the CDAP UI:

- **method:** Type of request to make when calling the plugin function from the CDAP UI
  (for example: GET or POST)
- **widget:** Type of widget to use to import output schema
- **output-property:** Property to update once the CDAP UI receives the data from the
  plugin method
- **plugin-method:** Name of the plugin method to call (as exposed by the plugin)
- **required-fields:** Fields required to call the plugin method
- **missing-required-fields-message:** A message for the user as to why the action is
  disabled in the CDAP UI, displayed when required fields are missing values

The last two properties (*required-fields* and *missing-required-fields-message*) are
solely for use by the CDAP UI and are not required for all widgets. However, the first four
fields are required fields to use a plugin method of the plugin in the CDAP UI. 

With plugin functions, if the widget is not supported in the CDAP UI or the
plugin function map is not supplied, the user will not see the widget in the CDAP UI.

Example Plugin
--------------
In the case of a *Batch Source* plugin example, the ``configuration-groups``, with
additional widgets to show the ``groupByFields`` and ``aggregates`` properties and using a
plugin-function, could be represented by::

  {
    "configuration-groups": [
      {
        "label": "Batch Source",
        "properties": [
          {
            "name": "name",
            "widget-type": "dataset-selector"
          },
          {
            "name": "basePath",
            "widget-type": "textbox"
          },
          {
            "name": "groupByFields",
            "widget-type": "textbox",
            "plugin-function": {
              "method": "POST",
              "widget": "outputSchema",
              "output-property": "schema",
              "plugin-method": "outputSchema",
              "required-fields": ["groupByFields", "aggregates"],
              "missing-required-fields-message":
                "Both 'Group By Fields' and 'Aggregates' properties are required to fetch the schema."
            }
          },
          {
            "name": "aggregates",
            "widget-type": "textbox"
          },
          {
            "name": "duration",
            "widget-type": "textbox"
          },
          {
            "name": "duration",
            "widget-type": "textbox"
          },
          ...
        ]
      }
    ]
  }


.. _plugins-presentation-inputs:

Inputs
------
Beginning with version 1.2 of the specification, a plugin can accept multiple input
schemas and from them generate a single output schema. Using the field ``multipleInputs``
and setting it to true tells the CDAP UI to show the multiple input schemas coming into a
specific plugin, instead of assuming that all of the schemas coming in from different
plugins are identical. 

This is an optional object, and if it is not present, it is assumed that all of the
schemas coming in from any connected plugins are identical. Currently, only one value
(``multipleInputs``) is accepted.

For example::

  {
    "metadata": {
      ...
    },
    "configuration-groups": [
      ...
    ],
    "inputs": {
      "multipleInputs": true
    },
    "outputs": [
      ...
    ]
  }

.. _plugins-presentation-outputs:

Outputs
-------
The *outputs* is a list of plugin properties that represent the output schema of a
particular plugin.

The output schema for a plugin can be represented in two different ways, either:

- via an *explicit schema* using a named property; or
- via an *implicit schema*

Output properties are configured in a similar manner as individual properties in
configuration groups. They are composed of a name and a widget-type, one of either
``schema`` (for an *explicit schema*) or ``non-editable-schema-editor`` (for an *implicit
schema*).

With the ``schema`` widget type, a list of widget attributes can be included; with the
``non-editable-schema-editor``, a schema to be displayed is added instead.

An **explicit schema** using a property can be defined as the output schema and then will
be editable through the CDAP UI.

For example, a "Batch Source" plugin could have a configurable output schema named
``data-format``, displayed for editing with the ``schema`` widget-type, with a default
type of ``string``, and a list of the types that are available::

  {
    "outputs": [
      {
        "name": "data-format",
        "widget-type": "schema",
        "widget-attributes": {
          "schema-default-type": "string",
          "schema-types": [
            "boolean",
            "int",
            "long",
            "float",
            "double",
            "string",
            "map<string, string>"
          ]
        }
      }
    ]
  }

An **implicit schema** is a pre-determined output schema for a plugin that the plugin
developer enforces. The implicit schema is not associated with any properties of the
plugin, but instead shows the output schema of the plugin, to be used for visual display
only.

An example of this is from the :github-hydrator-plugins:`KeyValueTable Batch Source plugin
<core-plugins/widgets/KVTable-batchsource.json>`::

  {
    "outputs": [
      {
        "widget-type": "non-editable-schema-editor",
        "schema": {
          "name": "etlSchemaBody",
          "type": "record",
          "fields": [
            {
              "name": "key",
              "type": "bytes"
            },
            {
              "name": "value",
              "type": "bytes"
            }
          ]
        }
      }
    ]
  }

Widget types for output properties are limited to ensure that the schema that is
propagated across different plugins in the CDAP UI is consistent.

.. _plugins-presentation-jumps:

Stream and Dataset Jumps
------------------------
Beginning with version 1.3 of the specification, a plugin can be specified (using
``jump-config``) with a map of stream and dataset "jumps". They specify which plugin
property names are either a stream or dataset that can be used, in the CDAP UI, to
directly jump to a detailed view of the stream or dataset.

This is an optional object, and if it is not present, no jump links will be created in the
CDAP UI. Jump links are not active in the CDAP Studio.

For example::

  {
    "metadata": {
      ...
    },
    "configuration-groups": [
      {
        "label": "KV Table Properties",
        "properties": [
          {
            "widget-type": "dataset-selector",
            "label": "Dataset Name",
            "name": "datasetName"
          }
        ]
      }
    ],
    "outputs": [
      {
        "widget-type": "non-editable-schema-editor",
        "schema": {
          "name": "etlSchemaBody",
          "type": "record",
          "fields": [
            {
              "name": "key",
              "type": "bytes"
            },
            {
              "name": "value",
              "type": "bytes"
            }
          ]
        }
      }
    ],
    "jump-config": {
      "datasets": [{
        "ref-property-name": "datasetName"
      }]
    }
  }  

In this example, the ``datasetName`` field of the ``dataset-selector`` will have a "jump" link added in the CDAP UI.

Example Widget JSON
===================
Based on the above specification, we can write a widget JSON for a *Batch Source* plugin
(with the properties of *name*, *basePath*, *duration*, *delay*, *groupByFields*,
*aggregates*, and an editable output *explicit schema*) as::

  {
    "metadata": {
      "spec-version": "<spec-version>"
    },
    "configuration-groups": [
      {
        "label": "Batch Source",
        "properties": [
          {
            "widget-type": "dataset-selector",
            "name": "name"
          },
          {
            "widget-type": "textbox",
            "name": "basePath"
          },
          {
            "widget-type": "textbox",
            "name": "duration"
          },
          {
            "widget-type": "textbox",
            "name": "delay"
          },
          {
            "widget-type": "textbox",
            "name": "groupByFields",
            "plugin-function": {
              "method": "POST",
              "widget": "outputSchema",
              "output-property": "schema",
              "plugin-method": "outputSchema",
              "required-fields": ["groupByFields", "aggregates"],
              "missing-required-fields-message":
                "Both 'Group By Fields' and 'Aggregates' properties are required to fetch the schema."
            }
          },
          {
            "widget-type": "keyvalue-dropdown",
            "name": "aggregates",
            "widget-attributes": {
              "showDelimiter": "false",
              "kv-delimiter" : ":",
              "delimiter" : ";",
              "dropdownOptions": [
                "Avg",
                "Count",
                "First",
                "Last",
                "Max",
                "Min",
                "Stddev",
                "Sum",
                "Variance"
              ]
            }
          }
        ]
      }
    ],
    "outputs": [
      {
        "name": "schema",
        "widget-type": "schema",
        "widget-attributes": {
          "schema-default-type": "string",
          "schema-types": [
            "boolean",
            "int",
            "long",
            "float",
            "double",
            "string",
            "map<string, string>"
          ]
        }
      }
    ]
  }


.. _plugins-presentation-specification-changes:

Specification Changes
=====================
These changes describe changes added with each version of the specification.

- **1.1:** Initial version of the specification.

- **1.2:** Added :ref:`multiple inputs <plugins-presentation-inputs>` for a plugin.

- **1.3:** Added :ref:`jump-config <plugins-presentation-jumps>` to specify which property
  names are to be connected in the CDAP UI to a detailed view of a stream or dataset.

- **1.4:** Added ``widget-type: hidden``.
