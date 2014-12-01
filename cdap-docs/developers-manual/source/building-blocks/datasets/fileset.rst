.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2014 Cask Data, Inc.

===================
The FileSet Dataset
===================

.. highlight:: java

A FileSet represents a set of files on the file system that share certain properties:

- The location in the file system. All files in a FileSet are located relative to a
  base path, which is created when the FileSet is created. Deleting the
  FileSet will also delete this directory and all the files it contains.
- The Hadoop input and output format. They are given as dataset properties by their
  class names, and whenever a FileSet is used as the input or output of a MapReduce,
  these classes are injected into the Hadoop configuration by the CDAP runtime
  system.
- Additional properties for the input and output format, for example, to specify
  the compression or the schema of the files in the Dataset. These properties are
  also set into the Hadoop configuration by the CDAP runtime system.

Note that these properties are configured once when the FileSet is created, and they
apply to all files in the Dataset. However, every time you use a FileSet in your
application code, you can address an individual file in the Dataset by specifying
its relative path as a runtime argument. This is only supported for MapReduce programs.

Support for FileSet is experimental in CDAP 2.6.0.

Creating a FileSet
==================

To use a FileSet in an application, you can create it as part of the the application
configuration::

  public class FileSetExample extends AbstractApplication {

    @Override
    public void configure() {
      ...
      createDataset("lines", FileSet.class, FileSetProperties.builder()
        .setBasePath("/input-lines")
        .setInputFormat(TextInputFormat.class)
        .setOutputFormat(TextOutputFormat.class).build());
      ...
    }

This creates a new FileSet named ``lines`` that uses TextInputFormat and TextOutputFormat.
Note that if you do not give a base path, the dataset framework will generate a path
from the dataset name. Also, if you do not specify an input format, you will not be able
to use this as the input for a MapReduce program (and similarly for the output format).

Using a FileSet in MapReduce
============================

Using a FileSet as input or output of a MapReduce is the same as for any other Dataset.
For example::

  public class WordCount extends AbstractMapReduce {

    @Override
    public void configure() {
      setInputDataset("lines");
      setOutputDataset("counts");
    }
    ...

The MapReduce only needs to specify the names of the input and output datasets. Whether
this is a FileSet or a different type of Dataset is handled by the CDAP runtime system.
However, you need to tell CDAP the relative paths of the input and output files. Currently
this is only possible by specifying them as runtime arguments when the MapReduce job is
started. For example::

  curl -v <base-url>/apps/FileSetExample/mapreduce/WordCount/start -d '{ \
      "dataset.lines.input.paths":  "monday/my.txt", \
      "dataset.counts.output.path": "monday/counts.out" }'

Note that for the input you can specify multiple paths separated by commas::

      "dataset.lines.input.paths":  "monday/lines.txt,tuesday/lines.txt"

If you do not specify the input and output path, your MapReduce job will fail with an error.

Using a FileSet Programmatically
================================

You can also interact with the files of a FileSet directly, through the Location abstraction
of the file system. For example, a Service can use a FileSet by declaring it in a @UseDataSet
statement, and then obtaining a Location for a relative path within the FileSet::

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
for more information about the Location abstraction.

