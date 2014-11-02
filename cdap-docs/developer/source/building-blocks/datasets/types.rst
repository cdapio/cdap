.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2014 Cask Data, Inc.

============================================
Types of Datasets
============================================

A Dataset abstraction is defined by a Java class that implements the ``DatasetDefinition`` interface.
The implementation of a Dataset typically relies on one or more underlying (embedded) Datasets.
For example, the ``IndexedTable`` Dataset can be implemented by two underlying Table Datasets –
one holding the data and one holding the index.

We distinguish three categories of Datasets: *core*, *system*, and *custom* Datasets:

- The **core** Dataset of the CDAP is a Table. Its implementation may use internal
  CDAP classes hidden from developers.

- A **system** Dataset is bundled with the CDAP and is built around
  one or more underlying core or system Datasets to implement a specific data pattern.

- A **custom** Dataset is implemented by you and can have arbitrary code and methods.
  It is typically built around one or more Tables (or other Datasets)
  to implement a specific data pattern.

Each Dataset is associated with exactly one Dataset implementation to
manipulate it. Every Dataset has a unique name and metadata that defines its behavior.
For example, every ``IndexedTable`` has a name and indexes a particular column of its primary table:
the name of that column is a metadata property of each Dataset of this type.

