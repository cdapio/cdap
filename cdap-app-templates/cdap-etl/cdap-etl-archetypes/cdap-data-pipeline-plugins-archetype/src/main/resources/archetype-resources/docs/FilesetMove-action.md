# FilesetMove Action

Description
-----------

Moves files from one FileSet into another FileSet.

Use Case
--------

This action may be used at the start of a pipeline run to move a subset of files from one FileSet into another
FileSet to process. Or it may be used at the end of a pipeline run to move a subset of files from the output FileSet
to some other location for further processing.

Properties
----------

**sourceFileset:** The name of the FileSet to move files from

**destinationFileSet:** The name of the FileSet to move files to

**filterRegex:** Filter any files whose name matches this regex.
Defaults to '^\\.', which filters any files that begin with a period.

Example
-------

This example moves files from the 'staging' FileSet into the 'input' FileSet.

    {
        "name": "TextFileSet",
        "type": "batchsource",
        "properties": {
            "sourceFileset": "staging",
            "destinationFileset": "input"
        }
    }
