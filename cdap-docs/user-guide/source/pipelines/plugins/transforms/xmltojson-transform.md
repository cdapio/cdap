# XML String to JSON String Converter


Description
-----------
Accepts a field that contains a properly-formatted XML string and 
outputs a properly-formatted JSON string version of the data. This is 
meant to be used with the Javascript transform for the parsing of 
complex XML documents into parts. Once the XML is a JSON string, you 
can convert it into a Javascript object using:

        var jsonObj = JSON.parse(input.jsonevent);


Configuration
-------------
**inputField:** Specifies the input field containing an XML string to 
be converted to a JSON string.

**outputField:** Specifies the output field where the JSON string will
be stored. If it is not present in the output schema, it will be
added. (Macro-enabled)

---
- CDAP Pipelines Plugin Type: transform
- CDAP Pipelines Version: 1.7.0
