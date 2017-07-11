# HDFS Delete


Description
-----------
Deletes a file or files within an HDFS cluster.


Use Case
--------
This action can be used to remove a file or files in an HDFS cluster.


Properties
----------
**path:** The full HDFS path of the file or files that need to be deleted. If the path points to a file, 
the file will be removed. If the path points to a directory with no regex specified, the directory and all of 
its contents will be removed. If a regex is specified, only the files and directories matching that regex
will be removed.

**fileRegex:** Wildcard regular expression to filter the files in the source directory that will be removed.

**continueOnError:** Indicates if the pipeline should continue if the delete process fails. If all files are not 
successfully deleted, the action will not re-create the files already deleted.


Example
-------
This example deletes all files ending in `.txt` from `/source/path`:

    {
        "name": "HDFSDelete",
        "plugin": {
            "name": "HDFSDelete",
            "type": "action",
            "artifact": {
                "name": "core-plugins",
                "version": "1.4.0-SNAPSHOT",
                "scope": "SYSTEM"
            },
            "properties": {
                "path": "hdfs://example.com:8020/source/path",
                "fileRegex": ".*\.txt",
                "continueOnError": "false"
            }
        }
    }

---
- CDAP Pipelines Plugin Type: action
- CDAP Pipelines Version: 1.7.0
