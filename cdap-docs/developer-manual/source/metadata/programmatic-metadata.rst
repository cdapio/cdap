.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2018 Cask Data, Inc.

.. _programmatic-metadata:

===================================
Accessing metadata programmatically
===================================

.. highlight:: java

Metadata can be retrieved and updated programmatically in both *programs* and *plugins*.

This feature can be used to perform metadata based processing. For example, suppose you have a dataset which contains
sensitive information in one of its fields. Your organization also has a policy that requires that all sensitive
information be masked. You can annotate the field with a tag to represent that it contains sensitive information.
You can then develop a plugin which reads the metadata of all the fields in the dataset and masks the contents of a
field, if it is annotated with a particular tag.

*Note*: Currently, reading metadata is not supported in a *program* or a *plugin* running in a *cloud* environment.
However, metadata can still be added, updated or deleted in this case.

.. _metadata-programs:

Programs
========
Metadata can be accessed from *MapReduce*, *Spark*, *Workers* and *Service* through methods from the
`MetadataReader <../../reference-manual/javadocs/io/cdap/cdap/api/metadata/MetadataReader.html>`__ object,
which are available via the appropriate program context object in your program. The program context object can
be obtained by invoking the ``getContext()`` method inside the ``initialize`` method of your program.

The following example shows how you can retrieve the metadata of an entity in a *MapReduce* program::

  @Override
  public void initialize() throws Exception {
    MapReduceContext context = getContext();

    // construct the metadata entity
    MetadataEntity entity = MetadataEntity.builder().append(MetadataEntity.NAMESPACE, "myNamespace")
      .appendAsType(MetadataEntity.DATASET, "myDataset").build();

    // get the metadata
    Map<MetadataScope, Metadata> metadata = context.getMetadata(entity);
  }

You can also annotate metadata to an entity through methods from the
`MetadataWriter <../../reference-manual/javadocs/io/cdap/cdap/api/metadata/MetadataWriter.html>`__
object, which are also available through the same program context object as above.

The following example shows how you can annotate metadata to entity in a *MapReduce* program::

  @Override
  public void initialize() throws Exception {
    MapReduceContext context = getContext();

    // construct the metadata entity
    MetadataEntity entity = MetadataEntity.builder().append(MetadataEntity.NAMESPACE, "myNamespace")
      .appendAsType(MetadataEntity.DATASET, "myDataset").build();

    // add a tag
    context.addTags(entity, "someTag");
  }

.. _metadata-plugins:

Plugins
=======
Metadata can be accessed from a *Plugin* through methods from the
`MetadataReader <../../reference-manual/javadocs/io/cdap/cdap/api/metadata/MetadataReader.html>`__ object,
which are available via the appropriate context provided to in ``prepareRun`` stage.

The following example shows how you can retrieve the metadata of an entity in a *Plugin*::

  @Override
  public void prepareRun(BatchSourceContext context) throws Exception {
    context.setInput(Input.ofDataset(config.tableName));

    // construct the metadata entity
    MetadataEntity entity = MetadataEntity.builder().append(MetadataEntity.NAMESPACE, "myNamespace")
      .appendAsType(MetadataEntity.DATASET, "myDataset").build();

    // get the metadata
    Map<MetadataScope, Metadata> metadata = context.getMetadata(entity);
  }

You can also annotate metadata to an entity through methods from the
`MetadataWriter <../../reference-manual/javadocs/io/cdap/cdap/api/metadata/MetadataWriter.html>`__
object, which are also available through the same context object as above.

The following example shows how you can annotate metadata to entity in a *Plugin*::

  @Override
  public void prepareRun(BatchSourceContext context) throws Exception {
    context.setInput(Input.ofDataset(config.tableName));

    // construct the metadata entity
    MetadataEntity entity = MetadataEntity.builder().append(MetadataEntity.NAMESPACE, context.getNamespace())
      .appendAsType(MetadataEntity.DATASET, config.tableName).build();

    // add a tag
    context.addTags(entity, "someTag");
  }
