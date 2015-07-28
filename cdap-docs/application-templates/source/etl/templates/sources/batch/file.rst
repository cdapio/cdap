.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2015 Cask Data, Inc.

====================
Sources: Batch: File 
====================

.. rubric:: Description: Batch source to use any Distributed File System as a Source

**Regex:** Regex to filter out filenames in the path.

To use the *TimeFilter*, input "timefilter". The TimeFilter assumes that it
is reading in files with the File log naming convention of YYYY-MM-DD-HH-mm-SS-Tag. The TimeFilter
reads in files from the previous hour if the timeTable field is left blank. So if it's currently
2015-06-16-15 (June 16th 2015, 3pm), it will read in files that contain 2015-06-16-14 in the filename.
If the field timeTable is present, then it will read files in that haven't been read yet.

.. highlight:: xml

**Filesystem Properties:** JSON of the properties needed for the
distributed file system. The formatting needs to be as follows::

  {
    "<property name>": "<property value>"
    ...
  }

For example, the property names needed for S3 are \"fs.s3n.awsSecretAccessKey\"
and \"fs.s3n.awsAccessKeyId\".

**Path:** Path to file(s) to be read. If a directory is specified,
terminate the path name with a \'/\'.

**Table:** Name of the Table that keeps track of the last time files
were read in.

**Input Format Class:** Name of the input format class, which must be a
subclass of FileInputFormat. Defaults to TextInputFormat.

**File System:** Distributed file system to read in from.
  