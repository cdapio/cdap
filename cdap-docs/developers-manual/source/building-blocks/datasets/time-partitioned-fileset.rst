.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2014-2016 Cask Data, Inc.

.. _datasets-timepartitioned-fileset:

=======================
TimePartitioned FileSet
=======================

A *TimePartitionedFileSet* is a special case (and in fact, a subclass) of ``PartitionedFileSet``, where
the partitioning is fixed to five integers representing the year, month, day of the month, hour of the day,
and minute of a partition's time. For convenience, it offers methods to address the partitions by
time instead of by partition key or filter. The time is interpreted as milliseconds since the Epoch.

These convenience methods provide access to partitions by time instead of by a partition key::

  @Nullable
  public TimePartition getPartitionByTime(long time);

  public Set<TimePartition> getPartitionsByTime(long startTime, long endTime);

  @Nullable
  public TimePartitionOutput getPartitionOutput(long time);

Essentially, these methods behave the same as if you had converted the time arguments into partition
keys and then called the corresponding methods of ``PartitionedFileSet`` with the resulting partition keys.
Additionally:

- The returned partitions have an extra method to retrieve the partition time as a long.
- The start and end times of ``getPartitionsByTime()`` do not correspond directly to a single partition filter,
  but to a series of partition filters. For example, to retrieve the partitions between November 2014 and
  March 2015, you need two partition filters: one for the months of November through December of 2014, and one
  for January through March of 2015. This method converts a given time range into the corresponding set
  of partition filters, retrieves the partitions for each filter, and returns the superset of all these
  partitions.

Using TimePartitionedFileSets in MapReduce
==========================================

Using time-partitioned file sets in MapReduce is similar to partitioned file sets; however, instead of
setting an input partition filter and an output partition key, you configure an input time range and an
output partition time in the ``initialize()`` of the MapReduce::

    TimePartitionedFileSetArguments.setInputStartTime(inputArgs, startTime);
    TimePartitionedFileSetArguments.setInputEndTime(inputArgs, endTime);

and::

    TimePartitionedFileSetArguments.setOutputPartitionTime(outputArgs, partitionTime);

You can achieve the same result by specifying the input time range and the output partition time
explicitly in the MapReduce runtime arguments. For example, you could give these arguments when starting
the MapReduce through a RESTful call::

  {
    "dataset.myInput.input.start.time": "1420099200000",
    "dataset.myInput.input.end.time": " 1422777600000",
    "dataset.results.output.partition.time": " 1422777600000",
  }

Note that the values for these times are milliseconds since the Epoch; the two times in this example represent
the midnight time of January 1st, 2015 and February 1st, 2015.

Exploring TimePartitionedFileSets
=================================

A time-partitioned file set can be explored with ad-hoc queries if you enable it at creation time,
similar to a FileSet, as described under :ref:`fileset-exploration`.
