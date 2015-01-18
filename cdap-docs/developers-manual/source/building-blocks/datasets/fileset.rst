.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2014-2015 Cask Data, Inc.

.. _datasets-fileset:

===============
FileSet Dataset
===============

.. highlight:: java

While realtime programs such as Flows normally require datasets with random access, batch-oriented
programming paradigms such as MapReduce are more suitable for data that can be read and written sequentially.
The most prominent form of such data is an HDFS file, and MapReduce is highly optimized for such files.
CDAP's abstraction for files is the FileSet Dataset.

A FileSet represents a set of files on the file system that share certain properties:

- The location in the file system. All files in a FileSet are located relative to a
  base path, which is created when the FileSet is created. Deleting the
  FileSet will also delete this directory and all the files it contains.
- The Hadoop input and output format. They are given as dataset properties by their
  class names.  When a FileSet is used as the input or output of a MapReduce program,
  these classes are injected into the Hadoop configuration by the CDAP runtime
  system.
- Additional properties of the specified input and output format. Each format has its own 
  properties; consult the format's documentation for details. For example, the
  ``TextOutputFormat`` allows configuring the field separator character by setting the
  property ``mapreduce.output.textoutputformat.separator``. These properties are also set
  into the Hadoop configuration by the CDAP runtime system.

These properties are configured at the time the FileSet is created. They apply to all
files in the Dataset. Every time you use a FileSet in your application code, you can
address either the entire Dataset or, by specifying its relative path as a runtime argument,
an individual file in the Dataset. Specifying an individual file is only supported for
MapReduce programs.

Support for FileSet datasets is experimental in CDAP 2.6.0.

Creating a FileSet
==================

To create and use a FileSet in an application, you create it as part of the application configuration::

  public class FileSetExample extends AbstractApplication {

    @Override
    public void configure() {
      ...
      createDataset("lines", FileSet.class, FileSetProperties.builder()
        .setBasePath("example/data/lines")
        .setInputFormat(TextInputFormat.class)
        .setOutputFormat(TextOutputFormat.class).build());
        .setOutputProperty(TextOutputFormat.SEPERATOR, ":")
      ...
    }

This creates a new FileSet named *lines* that uses ``TextInputFormat`` and ``TextOutputFormat.``
For the output format, we specify an additional property to make it use a colon as the separator
between the key and the value in each line of output.

Input and output formats must be implementations of the standard Apache Hadoop
`InputFormat <https://hadoop.apache.org/docs/current/api/org/apache/hadoop/mapreduce/InputFormat.html>`_
and
`OutputFormat <https://hadoop.apache.org/docs/current/api/org/apache/hadoop/mapreduce/OutputFormat.html>`_
specifications.

If you do not specify a base path, the dataset framework will generate a path
based on the dataset name. If you do not specify an input format, you will not be able
to use this as the input for a MapReduce program; similarly, for the output format.


Using a FileSet in MapReduce
============================

Using a FileSet as input or output of a MapReduce program is the same as for any other Dataset::

  public class WordCount extends AbstractMapReduce {

    @Override
    public void configure() {
      setInputDataset("lines");
      setOutputDataset("counts");
    }
    ...

The MapReduce program only needs to specify the names of the input and output datasets.
Whether they are FileSets or another type of Dataset is handled by the CDAP runtime system.

However, you do need to tell CDAP the relative paths of the input and output files. Currently,
this is only possible by specifying them as runtime arguments when the MapReduce program is started::

  curl -v <base-url>/apps/FileSetExample/mapreduce/WordCount/start -d '{ \
      "dataset.lines.input.paths":  "monday/my.txt", \
      "dataset.counts.output.path": "monday/counts.out" }'

Note that for the input you can specify multiple paths separated by commas::

      "dataset.lines.input.paths":  "monday/lines.txt,tuesday/lines.txt"

If you do not specify both the input and output paths, your MapReduce program will fail with an error.

Using a FileSet Programmatically
================================

You can interact with the files of a FileSet directly, through the ``Location`` abstraction
of the file system. For example, a Service can use a FileSet by declaring it with a ``@UseDataSet``
annotation, and then obtaining a ``Location`` for a relative path within the FileSet::

    @UseDataSet("lines")
    private FileSet lines;

    @GET
    @Path("{fileSet}")
    public void read(HttpServiceRequest request, HttpServiceResponder responder,
                     @QueryParam("path") String filePath) {

      Location location = lines.getLocation(filePath);
      try {
        InputStream inputStream = location.getInputStream();
        ...
      } catch (IOException e) {
        responder.sendError(400, String.format("Unable to read path '%s'", filePath));
        return;
      }
    }

See the Apache™ Twill®
`API documentation <http://twill.incubator.apache.org/apidocs/org/apache/twill/filesystem/Location.html>`__
for additional information about the ``Location`` abstraction.

