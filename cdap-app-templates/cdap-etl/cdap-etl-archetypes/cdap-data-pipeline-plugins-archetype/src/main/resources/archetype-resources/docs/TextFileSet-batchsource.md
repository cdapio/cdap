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

Example
-------

This example reads from a FileSet named 'users' and deletes the data it read if the pipeline run succeeded:

    {
        "name": "TextFileSet",
        "type": "batchsource",
        "properties": {
            "fileSetName": "users",
            "deleteInputOnSuccess": "true"
        }
    }

In order to properly read from the FileSet, the runtime argument 'dataset.<name>.input.paths' should be set.
In the example above, setting 'dataset.users.output.path' to 'run10' will configure the pipeline run to read from
to the 'run10' directory in the FileSet.
