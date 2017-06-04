.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2017 Cask Data, Inc.

.. _datasets-permissions:

===================
Dataset Permissions
===================

In a multi-tenant environment, it is often necessary to control access to data. Datasets allow
for the control of access permissions through dataset properties. When a dataset is created, the
underlying storage resources (for example: directories or tables) are created with the
configured permissions. When data is added to a dataset (for example: a new file), the same
permissions are applied.

Due to the different characteristics of the underlying storage providers, permissions are
configured differently depending on the dataset type.


FileSet Permissions
===================
FileSets store their data as files and directories in the file system. The typical access
control used in file systems is by assigning different permissions to the file's owner,
the file's group, and all others. For example, in a Posix file system, the permission string::

    rwxr-x---

gives read, write, and execute permission to the owner, read and execute permissions to the group,
and no permissions to the rest of the world. This can also be expressed as a three-digit
octal number::

    750

where each bit represents the presence or absence of a permission in the string representation.

A FileSet can be configured to set the group and permissions of all directories and files that
are created through its API::

    createDataset("lines", FileSet.class, FileSetProperties.builder()
      .setBasePath("example/data/lines")
      .setFileGroup("subscribers")
      .setFilePermissions("rwxr-x---")
      .setInputFormat(TextInputFormat.class)
      .setOutputFormat(TextOutputFormat.class)
      .setDescription("Store input lines")
      .build());

This sets the group name to ``subscribers`` and permits all members of that group to read
files and directories (the execute bit |---| ``x`` |---| is required to operate on a directory).
Any files created through this FileSet will now have these permissions. For example::

    location = fileset.getLocation(path);
    try (OutputStream out = location.getOutputStream) {
       ...
    }

This will create a new file at the requested path with the permissions and group name
configured in the dataset properties.

Similarly, if you write to the FileSet using a MapReduce program, the output directories
and files will have the proper permissions, except that |---| by convention |---| Hadoop
removes the execute bits from plain files, and it sets the parent directory's group name
for all new files it creates.

If a MapReduce program has multiple output datasets that each configure their own permissions,
then we encounter a limitation in MapReduce: it only allows a single set of permissions for
each of its tasks. If the permissions of the output FileSets differ, then it is hard to predict
which one will prevail |---| every mapper or reducer might even apply a different one. Therefore,
CDAP applies this heuristic:

- If all output datasets specify the same permissions, then these permissions are used.
 
- If at least two of the output datasets specify different permissions, then the default permissions
  of the file system are used (and a warning is issued in the log).

To circumvent this, you can set a fixed set of permissions in the job configuration, by setting
the file system's ``umask``::

    @Override
    public void initialize() throws Exception {
      MapReduceContext context = getContext();
      Job job = context.getHadoopJob();
      job.getConfiguration().set(FsPermission.UMASK_LABEL, "027"); // "fs.permissions.umask-mode"
      context.addOutput(Output.of("fileset1", ...));
      context.addOutput(Output.of("fileset2", ...));
      ...
    }

This will override any permissions configured for output datasets. Be aware that the ``umask`` is
the inverse of a file permission: the above ``umask`` of ``027`` translates into file
permissions of ``750`` or ``rwxr-x---``.


Table Permissions
=================
Table-based datasets are typically backed by HBase as the storage provider. HBase controls
access by granting privileges explicitly to users and groups. This is more flexible than
Unix-style file system permissions, as it allows a fine-grained control over multiple
groups or individual users.

To configure permissions for a Table dataset, set them in the dataset properties::

    createDataset("myTable", Table.class, TableProperties.builder()
      .setTTL(100000)
      .setTablePermissions(ImmutableMap.of("joe", "rwa", "@subscribers", "r"))
      .build());

This gives user ``joe`` read, write, and administration rights, while all members of the
group ``subscribers`` can only read. Other privileges you can assign are ``c`` (for create)
and ``x`` (for execute rights); see the `Apache HBase™ documentation 
<http://hbase.apache.org/book.html#hbase.accesscontrol.configuration>`__ for more details.


PartitionedFileSet Permissions
==============================
Partitioned file sets and Time-partitioned file sets are a hybrid of a Table to store partition
metadata and a FileSet to store the partition data (that is, files). You can set both Table
permissions and FileSet permissions in the dataset properties for these dataset types, and they
will be applied to the partition table and the file system data, respectively.
