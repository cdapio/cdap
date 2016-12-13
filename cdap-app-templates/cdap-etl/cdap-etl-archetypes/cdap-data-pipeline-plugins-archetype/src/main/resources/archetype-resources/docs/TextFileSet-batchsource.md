# Text FileSet Batch Source

Description
-----------

Reads from a CDAP FileSet in text format. Outputs records with two fields -- position (long), and text (string).

Use Case
--------

This source is used whenever you need to read from a FileSet in text format.

Properties
----------

**fileSetName:** The name of the FileSet to read from.

**createIfNotExists:** Whether to create the FileSet if it does not exist. Defaults to false.

**deleteInputOnSuccess:** Whether to delete the data read if the pipeline run succeeded. Defaults to false.

**files:** A comma separated list of files in the FileSet to read. Macro enabled.

Example
-------

This example reads from a FileSet named 'users' and deletes the data it read if the pipeline run succeeded:

    {
        "name": "TextFileSet",
        "type": "batchsource",
        "properties": {
            "fileSetName": "users",
            "deleteInputOnSuccess": "true",
            "files": "${inputFiles}"
        }
    }

Before running the pipeline, the 'inputFiles' runtime argument must be specified.
