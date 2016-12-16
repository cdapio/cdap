# FilesetDelete Post Action

Description
-----------

If a pipeline run succeeds, deletes files in a FileSet that match a configurable regex.

Use Case
--------

This post action is used if you need to clean up some files after a successful pipeline run.

Properties
----------

**filesetName:** The name of the FileSet to delete files from.

**directory:** The directory in the FileSet to delete files from. Macro enabled.

**deleteRegex:** Delete files that match this regex.

Example
-------

This example deletes any files that have the '.crc' extension from the 2016-01-01 directory of a FileSet named 'users'.

    {
        "name": "TextFileSet",
        "type": "batchsource",
        "properties": {
            "fileSetName": "users",
            "directory": "2016-01-01",
            "deleteRegex": ".*\\.crc"
        }
    }
