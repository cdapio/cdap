.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2017 Cask Data, Inc.
    :description: The CDAP User Guide

.. _user-guide-data-preparation-parsers-hl7:

==========
HL7 Parser
==========

#

PARSE-AS-HL7 is a directive for parsing Health Level 7 (HL7) messages.
HL7 is a messaging standard used in the healthcare industry to exchange data between systems.

## Syntax

```
parse-as-hl7 <column>
```

```column``` contains HL7 messages.

## Usage Notes

The following is an example of an HL7 message in the record:

```
{
"body" : "MSH|^~\&||.|||199908180016||ADT^A04|ADT.1.1698593|P|2.7
PID|1||000395122||LEVERKUHN^ADRIAN^C||19880517180606|M|||6 66TH AVE NE^^WEIMAR^DL^98052||(157)983-3296|||S||12354768|87654321
NK1|1|TALLIS^THOMAS^C|GRANDFATHER|12914 SPEM ST^^ALIUM^IN^98052|(157)883-6176
NK1|2|WEBERN^ANTON|SON|12 STRASSE MUSIK^^VIENNA^AUS^11212|(123)456-7890
IN1|1|PRE2||LIFE PRUDENT BUYER|PO BOX 23523^WELLINGTON^ON^98111|||19601||||||||THOMAS^JAMES^M|F|||||||||||||||||||ZKA535529776"
}
```

Data in an HL7 message is organized hierarchically as follows:

* Message = { Segment, Segment, ... Segment }
* Segment = { Field, Field, ... Field }
* Field = { Component, ... Component }
* Component = {Sub-Component, ... Sub-Component }

Each line of an HL7 message is a segment. A segment is a logical grouping of fields. The first three characters in a
segment identify the segment type. In the above message, there are five segments MSH (message header), PID (patient
identification), two NK1 (next of kin) segments, and IN1 (insurance).

Each segment consists of fields. A field contains information related to the purpose of the segment, such as the name
of the insurance company in the IN1 (insurance) segment. Fields are typically (but not always) delimited by a |
character.

Fields can be divided into components. Components are typically indicated by a ^ character. In the above example,
the PID (patient identification) segment contains a patient name field containing LEVERKUHN^ADRIAN^C which has three
parts, last name (LEVERKUHN), first name (ADRIAN), and middle initial (C). Components can be divided into
subcomponents. Subcomponents are typically indicated by a & character.

Parsing the above message using the directive will generate a record that has flattened HL7 fields and all
the **fields are converted to JSON**. Further parsing can be achieved with [PARSE-AS-JSON](parse-as-json.md) directive

```
parse-as-hl7 message
```

Would generate the following record

```
{
"body" : "<original hl7 message>",
"body.hl7" : {
"MSH" : [
{
"<field index>" : "<primitive value>",
"<field index>" : "<primitive value>",
"<field index>" : {
"<component index>" : "<primitive value>",
"<component index>" : "<primitive value>",
...
"<component index>" : "<primitive value>",
}
}
],
"PID" : [
{
...
}
]
"NK1" : [
{
}
],
"IN1" : [
{
...
}
]
}
}
```

Once it's converted into JSON for each segment, you can apply [parse-as-json](parse-as-json.md) or [json-path](json-path.md) directives on the record.
