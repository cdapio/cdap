# Excel


Description
-----------
The Excel plugin provides user the ability to read data from one or more Excel file(s).

The plugin supports following types of Excel file(s):
Microsoft Excel 97(-2007) file format
Microsoft Excel XML (2007+) file format


Use Case
--------
The Excel plugin is used to read excel file(s) and converts the rows to structured records based
on the column names, column-label mapping and column-type mapping provided by the user. Also keeps track
of all the processed excel files in a memory table provided by the user. So, that if user has the option
not to reprocess a particular file.


Properties
----------

**filePath:** Path of the excel file(s) to be read. (Macro-enabled) Supports below formats:

      Microsoft Excel 97(-2007) file format
      Microsoft Excel XML (2007+) file format

**filePattern:** Regex pattern to select specific excel file(s) from the path provided
in **filePath** input. (Macro-enabled)

**memoryTableName:** KeyValue table name to keep the track of processed files. This can be
a new table or existing one. (Macro-enabled)

**reprocess:** Specify whether the files mentioned in the memory table should be reprocessed or not.

**sheet:** Specifies whether sheet has to be processed by sheet name or sheet number.

**sheetValue:** Specifies the value corresponding to 'sheet' input. Value can be either actual
sheet name or sheet number.
for example: 'Sheet1' or '0' in case user selects 'Sheet Name' or 'Sheet Number' as 'sheet'
input respectively. Sheet number starts with 0. (Macro-enabled)

**columnList:** Specify the excel column names which needs to be extracted from the excel sheet.
Column name has to be same as excel column name; for example: A, B, etc.

**columnMapping:** List of the excel column names to be renamed. The key specifies the name of the
excel column to be renamed, with its corresponding value specifying the new name for that column.
Column name has to be same as excel column name; for example: A, B, etc.

**skipFirstRow:** Specify whether the first row in the excel sheet needs to be processed or not.

**terminateIfEmptyRow:** Specify whether processing needs to be terminated in case an empty row is
encountered while processing excel files.

**rowsLimit:** Maximum row limit for each sheet to be processed. If, the limit is not provided then
all the rows in the sheet will be processed. (Macro-enabled)

**outputSchema:** Mapping of excel column names in the output schema to data types. Consists of
a comma-separated list. This input is mandatory if no inputs for 'columnList' has been provided.
Column name has to be same as excel column name; for example: A, B, etc.

If type has not been provided for a column mentioned in **columnList** input, then output data type
of that column will be **string**.

**ifErrorRecord:** Specifies the action to be take in case of an error. (Macro-enabled)

**errorDatasetName:** Table name to keep the error record encountered while processing the excel file(s). (Macro-enabled)


Condition
---------

1. To process an excel sheet, either of **columnList** or **outputSchema** is mandatory.
2. If all the columns needs to be processed, then **columnList** or **outputSchema** can be used to specify the column
   names.


Example
-------

This example reads all files with pattern **.*** from a hdfs path "hdfs://<namenode-hostname>:9000/cdap"  and parses it
using the column list, column-label mapping and column-type mapping. It also keeps track of the processed
file name in specified memory table. It will drop columns other than the one mentioned in **columnList** and
generate structured records according to the inputs.

The plugin JSON Representation will be:

    {
      "name": "Excel",
      "type": "batchsource",
      "properties": {
            "filePath": "hdfs://<namenode-hostname>:9000/cdap",
            "filePattern": ".*",
            "memoryTableName": "inventory-memory-table",
            "reprocess": "false",
            "sheet": "Sheet1",
            "sheetValue": "-1",
            "columnList": "A,B",
            "columnMapping": "B:name,C:age"
            "skipFirstRow": "false",
            "terminateIfEmptyRow": "false",
            "rowsLimit": "" ,
            "outputSchema": "A:string",
            "ifErrorRecord" : "Ignore error and continue",
            "errorDatasetName": ""
       }
    }

Suppose, the above **filePath** contains only one file with these input rows from **Sheet1**:

    +======================================+
    |    A      |     B      |     C       |
    +======================================+
    |    1      |  "John"    |    23       |
    |    2      |  "Alan"    |    34       |
    |    3      |  "Ashley"  |    45       |
    +======================================+

After, the processing has been done by the Excel plugin, the output will have these
structure and contents, with the 'B' and 'C' column names being replaced by the 'name' and 'age'
columns respectively:

    +======================================+
    |    A       |   name    |     age     |
    +======================================+
    |   "1"      |  "John"   |    "23"     |
    |   "2"      |  "Alan"   |    "34"     |
    |   "3"      |  "Ashley" |    "45"     |
    +======================================+


The memory table **inventory-memory-table** will contain:

    +===========================================================================+
    |    key                                                 |   value          |
    +===========================================================================+
    | "hdfs://<namenode-hostname>:9000/tmp/inventory.xlsx"   | "1322018752992l" |
    +===========================================================================+

---
- CDAP Pipelines Plugin Type: batchsource
- CDAP Pipelines Version: 1.7.0
