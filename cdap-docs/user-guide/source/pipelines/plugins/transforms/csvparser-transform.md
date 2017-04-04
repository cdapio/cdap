# CSV Parser


Description
-----------
Parses an input field as a CSV Record into a Structured Record. Supports multi-line CSV Record parsing
into multiple Structured Records. Different formats of CSV Record can be parsed using this plugin.
Supports these CSV Record types: ``DEFAULT``, ``EXCEL``, ``MYSQL``, ``RFC4180``, ``Tab Delimited``, ``Pipe Delimited``
and ``Custom``.


Configuration
-------------
**format:** Specifies the format of the CSV Record the input should be parsed as.

**delimiter:** Custom delimiter to be used for parsing the fields. The custom delimiter can only be specified by 
selecting the option 'Custom' from the format drop-down. In case of null, defaults to ",".

**field:** Specifies the input field that should be parsed as a CSV Record. 
Input records with a null input field propagate all other fields and set fields that
would otherwise be parsed by the CSVParser to null.

**schema:** Specifies the output schema of the CSV Record.

**errorDataset:**  If error dataset is configured then all the errored rows, if present in the CSV, will be committed
 to the specified error dataset. If not configured, the errored rows will be committed to default error dataset.

---
- CDAP Pipelines Plugin Type: transform
- CDAP Pipelines Version: 1.7.0
