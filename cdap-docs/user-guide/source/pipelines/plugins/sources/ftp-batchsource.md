# FTP


Description
-----------
Batch source for an FTP or SFTP source. Prefix of the path ('ftp://...' or 'sftp://...') determines the source server
type, either FTP or SFTP.


Use Case
--------
This source is used whenever you need to read from an FTP or SFTP server.


Properties
----------
**referenceName:** This will be used to uniquely identify this source for lineage, annotating metadata, etc.

**path:** Path to file(s) to be read. The path uses filename expansion (globbing) to read files.
Path is expected to be of the form prefix://username:password@hostname:port/path (Macro-enabled)

**fileRegex:** Regex to filter out files in the path. It accepts regular expression which is applied to the complete
path and returns the list of files that match the specified pattern. (Macro-enabled)

**fileSystemProperties:** A JSON string representing a map of properties
needed for the distributed file system. (Macro-enabled)

**inputFormatClassName:** Name of the input format class, which must be a subclass of FileInputFormat.
Defaults to CombineTextInputFormat. (Macro-enabled)

**ignoreNonExistingFolders:** Identify if path needs to be ignored or not, for case when directory or file does not
exists. If set to true it will treat the not present folder as 0 input and log a warning. Default is false.

**recursive:** Boolean value to determine if files are to be read recursively from the path. Default is false.


Example
-------
This example connects to an SFTP server and reads in files found in the specified directory.

    {
        "name": "FTP",
        "type": "batchsource",
        "properties": {
            "path": "sftp://username:password@hostname:21/path/to/logs",
            "ignoreNonExistingFolders": "false",
            "recursive": "false"
        }
    }

---
- CDAP Pipelines Plugin Type: batchsource
- CDAP Pipelines Version: 1.7.0
