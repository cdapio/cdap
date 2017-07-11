.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2014-2017 Cask Data, Inc.

============
Parse as HL7
============

The PARSE-AS-HL7 directive is for parsing Health Level 7 Version 2 (HL7
V2) messages. `HL7 <http://www.hl7.org>`__ is a messaging standard used
in the healthcare industry to exchange data between systems.

Syntax
------

::

    parse-as-hl7 <column>

The ``<column>`` contains HL7 V2 messages, v2.1 through v2.6.

Usage Notes
-----------

This is an example of an `HL7
V2 <http://www.hl7.org/implement/standards/product_brief.cfm?product_id=185>`__
message in a record:

::

    MSH|^~\&||.|||199908180016||ADT^A04|ADT.1.1698593|P|2.6
    PID|1||000395122||LEVERKUHN^ADRIAN^C||19880517180606|M|||6 66TH AVE NE^^WEIMAR^DL^98052||(157)983-3296|||S||12354768|87654321
    NK1|1|TALLIS^THOMAS^C|GRANDFATHER|12914 SPEM ST^^ALIUM^IN^98052|(157)883-6176
    NK1|2|WEBERN^ANTON|SON|12 STRASSE MUSIK^^VIENNA^AUS^11212|(123)456-7890
    IN1|1|PRE2||LIFE PRUDENT BUYER|PO BOX 23523^WELLINGTON^ON^98111|||19601||||||||THOMAS^JAMES^M|F|||||||||||||||||||ZKA535529776

Data in an HL7 V2 message is organized hierarchically as:

-  Message = { Segment, Segment, ... Segment }
-  Segment = { Field, Field, ... Field }
-  Field = { Component, Component, ... Component }
-  Component = { Sub-Component, Sub-Component, ... Sub-Component }

Each line of an HL7 V2 message is a segment. A segment is a logical
grouping of fields. The first three characters in a segment identify the
segment type. In the above message, there are five segments: the ``MSH``
(message header); the ``PID`` (patient identification); two ``NK1``
(next-of-kin) segments; and the ``IN1`` (insurance).

Each segment consists of fields. A field contains information related to
the purpose of the segment, such as the name of the insurance company in
the ``IN1`` (insurance) segment. Fields are typically (but not always)
delimited by a ``|`` character.

Fields can be divided into components. Components are typically
indicated by a ``^`` character. In the above example, the ``PID``
(patient identification) segment contains a patient name field
containing ``LEVERKUHN^ADRIAN^C`` which has three parts: last name
(``LEVERKUHN``); first name (``ADRIAN``); and middle initial (``C``).
Components can be divided into subcomponents. Subcomponents are
typically indicated by an ``&`` character.

Parsing the above message using the directive will generate a record
that has flattened HL7 V2 components and all **fields converted to
JSON**. Further parsing can be achieved with the
`PARSE-AS-JSON <parse-as-json.md>`__ directive.

Example
-------

Applying this directive to the above message:

::

    parse-as-hl7 body

would result in a record of this form:

::

    {
      "body": "<original-hl7-message>",
      "body_hl7_MSH_<field-index>": "<primitive-value>",
      ...
      "body_hl7_MSH_<field-index>_<component-index>": "<primitive-value>",
      ...
      "body_hl7_PID_<field-index>": "<primitive-value>",
      ...
      "body_hl7_PID_<field-index>_<component-index>": "<primitive-value>",
      ...
      "body_hl7_NK1_<field-index>>": "<primitive-value>",
      ...
      "body_hl7_NK1_<field-index>_<component-index>": "<primitive-value>",
      ...
      "body_hl7_IN1_<field-index>>": "<primitive-value>",
      ...
      "body_hl7_IN1_<field-index>_<component-index>": "<primitive-value>",
      ...
    }

Once each segment has been converted into JSON, you can apply
`PARSE-AS-JSON <parse-as-json.md>`__ or `JSON-PATH <json-path.md>`__
directives on the record.
