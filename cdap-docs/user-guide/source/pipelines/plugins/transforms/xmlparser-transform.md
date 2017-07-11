# XML Parser

Description
-----------
The XML Parser Transform uses XPath to extract fields from a complex XML event. This plugin should generally be used
in conjunction with the XML Reader Batch Source. The XML Reader will provide individual events to the XML Parser,
which will be responsible for extracting fields from the events and mapping them to the output schema.


Use Case
--------
The transform takes an input record that contain XML events or records, parses it using the specified XPaths and returns
a structured record according to the specified schema. For example, this plugin can be used in conjunction with the XML
Reader Batch Source to extract values from XMLNews documents and create structured records which are easier to query.


Properties
----------

**input:** The field in the input record that is the source of the XML event or record. (Macro-enabled)

**encoding:** The source XML character set encoding (default UTF-8). (Macro-enabled)

**xPathMappings:** Mapping of the field names to the XPaths of the XML record. A comma-separated list, each element of
which is a field name, followed by a colon, followed by an XPath expression. XPath location paths can include predicates
and supports XPath 1.0.
Example : ``<field-name>:<XPath expression>``

**fieldTypeMapping:** Mapping of field names in the output schema to data types. Consists of a comma-separated list,
each element of which is a field name followed by a colon and a type, where the field names are the same as used in the
xPathMappings, and the type is one of: boolean, int, long, float, double, bytes, or string.
Example : ``<field-name>:<data-type>``

**processOnError:** The action to take in case of an error.
                     - "Ignore error and continue"
                     - "Exit on error" : Stops processing upon encountering an error
                     - "Write to error dataset" :  Writes the error record to an error dataset and continues

**failOnArray:** Whether to allow xpaths that are arrays. If false, the first element will be chosen. Defaults to false.

Example
-------

This example parses an XML record received in the "body" field of the input record following the *xPathMappings* for
each field name. The output structured record will be created using the type specified for each field in the
"fieldTypeMapping". Only years and prices will be passed on for books with a price over 35.00:

       {
            "name": "XMLParser",
            "plugin": {
                "name": "XMLParser",
                "type": "transform",
                "label": "XMLParser",
                "properties": {
                    "input": "body",
                    "encoding": "UTF-8",
                    "xPathMappings": "category://book/@category,
                                      title://book/title,
                                      year:/bookstore/book[price>35.00]/year,
                                      price:/bookstore/book[price>35.00]/price,
                                      subcategory://book/subcategory",
                    "fieldTypeMapping": "category:string,title:string,year:int,price:double,subcategory:string",
                    "processOnError": "Ignore error and continue"
                }
            }
       }

For example, suppose the transform receives these input records:

    +=========================================================================================================+
    | offset   | body                                                                                         |
    +=========================================================================================================+
    | 1        | <bookstore><book category="cooking"><subcategory><type>Continental</type><genre>European     |
    |          | cuisines</genre></subcategory><title lang="en">Everyday Italian</title><author>Giada De      |
    |          | Laurentiis</author><year>2005</year><price>30.00</price></book></bookstore>                  |
    | 2        | <bookstore><book category="children"><subcategory><type>Series</type><genre>fantasy          |
    |          | literature</genre></subcategory><title lang="en">Harry Potter</title><author>J. K. Rowling   |
    |          | </author><year>2005</year><price>49.99</price></book></bookstore>                            |
    +=========================================================================================================+

The output records will contain:

    +=========================================================================================================+
    | category  | title              | year   |  price  | subcategory                                         |
    +=========================================================================================================+
    | cooking   | Everyday Italian   | null   |  null   | <subcategory><type>Continental</type><genre>European|
    |           |                    |        |         | cuisines</genre></subcategory>                      |
    | children  | Harry Potter       | 2005   | 49.99   | <subcategory><type>Series</type><genre>fantasy      |
    |           |                    |        |         | literature</genre></subcategory>                    |
    +=========================================================================================================+

Here, since the subcategory contains child nodes, the plugin will return the complete subcategory node (along with its
child elements) as string as ``<subcategory><type>Continental</type><genre>European cuisines</genre></subcategory>`` .
This is to ensure that the plugin returns a single XML event for a structured record intead of the two child events:
 ``<type>Continental</type>`` and ``<genre>European cuisines</genre>``.

---
- CDAP Pipelines Plugin Type: transform
- CDAP Pipelines Version: 1.7.0
