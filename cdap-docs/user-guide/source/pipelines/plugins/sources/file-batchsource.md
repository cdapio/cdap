# File


Description
-----------
Batch source to use any Distributed File System as a Source.


Use Case
--------
This source is used whenever you need to read from a distributed file system.
For example, you may want to read in log files from S3 every hour and then store
the logs in a TimePartitionedFileSet.


Properties
----------
**referenceName:** This will be used to uniquely identify this source for lineage, annotating metadata, etc.

**fileSystem:** Distributed file system to read in from.

**fileSystemProperties:** A JSON string representing a map of properties
needed for the distributed file system.
For example, the property names needed for S3 are "fs.s3n.awsSecretAccessKey"
and "fs.s3n.awsAccessKeyId". (Macro-enabled)

**path:** Path to file(s) to be read. If a directory is specified,
terminate the path name with a '/'. The path uses filename expansion (globbing) to read files. (Macro-enabled)

**fileRegex:** Regex to filter out files in the path. It accepts regular expression which is applied to the complete
path and returns the list of files that match the specified pattern.
To use the *TimeFilter*, input ``timefilter``. The TimeFilter assumes that it is
reading in files with the File log naming convention of *YYYY-MM-DD-HH-mm-SS-Tag*.
The TimeFilter reads in files from the previous hour if the field ``timeTable`` is
left blank. If it's currently *2015-06-16-15* (June 16th 2015, 3pm), it will read
in files that contain *2015-06-16-14* in the filename. If the field ``timeTable`` is
present, then it will read in files that have not yet been read. (Macro-enabled)

**pathField:** If specified, each output record will include a field with this name that contains the file URI
that the record was read from. Requires a customized version of CombineFileInputFormat, so it cannot be used if
an inputFormatClass is given.

**filenameOnly:** If true and a pathField is specified, only the filename will be used.
If false, the full URI will be used. Defaults to false.

**timeTable:** Name of the Table that keeps track of the last time files
were read in. (Macro-enabled)

**inputFormatClass:** Name of the input format class, which must be a subclass of FileInputFormat.
Cannot be used if pathField is set. (Macro-enabled)

**maxSplitSize:** Maximum split-size for each mapper in the MapReduce Job. Defaults to 128MB. (Macro-enabled)

**ignoreNonExistingFolders:** Identify if path needs to be ignored or not, for case when directory or file does not
exists. If set to true it will treat the not present folder as 0 input and log a warning. Default is false.

**recursive:** Boolean value to determine if files are to be read recursively from the path. Default is false.

Example
-------
This example connects to Amazon S3 and reads in files found in the specified directory while
using the stateful Timefilter, which ensures that each file is read only once. The Timefilter
requires that files be named with either the convention ``"yy-MM-dd-HH..."`` (S3) or ``"...'.'yy-MM-dd-HH..."``
(Cloudfront). The stateful metadata is stored in a table named 'timeTable'. With the maxSplitSize
set to 1MB, if the total size of the files being read is larger than 1MB, CDAP will
configure Hadoop to use more than one mapper:

    {
        "name": "FileBatchSource",
        "type": "batchsource",
        "properties": {
            "fileSystem": "S3",
            "fileSystemProperties": "{
                \"fs.s3n.awsAccessKeyId\": \"accessID\",
                \"fs.s3n.awsSecretAccessKey\": \"accessKey\"
            }",
            "path": "s3n://path/to/logs/",
            "fileRegex": "timefilter",
            "timeTable": "timeTable",
            "maxSplitSize": "1048576",
            "ignoreNonExistingFolders": "false",
            "recursive": "false"
        }
    }

---
- CDAP Pipelines Plugin Type: batchsource
- CDAP Pipelines Version: 1.7.0
