/*
 * Copyright © 2014 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package co.cask.cdap.data2.dataset2.lib.partitioned;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.dataset.DataSetException;
import co.cask.cdap.api.dataset.DatasetSpecification;
import co.cask.cdap.api.dataset.lib.FileSet;
import co.cask.cdap.api.dataset.lib.PartitionFilter;
import co.cask.cdap.api.dataset.lib.PartitionKey;
import co.cask.cdap.api.dataset.lib.PartitionedFileSetProperties;
import co.cask.cdap.api.dataset.lib.Partitioning;
import co.cask.cdap.api.dataset.lib.TimePartitionedFileSet;
import co.cask.cdap.api.dataset.lib.TimePartitionedFileSetArguments;
import co.cask.cdap.api.dataset.table.Put;
import co.cask.cdap.api.dataset.table.Row;
import co.cask.cdap.api.dataset.table.Scanner;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.explore.client.ExploreFacade;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Supplier;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.inject.Provider;
import org.apache.twill.filesystem.Location;
import org.slf4j.LoggerFactory;

import java.util.Calendar;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Implementation of partitioned datasets using a Table to store the meta data.
 */
public class TimePartitionedFileSetDataset extends PartitionedFileSetDataset implements TimePartitionedFileSet {

  private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(TimePartitionedFileSetDataset.class);

  // the fixed partitioning that time maps to
  private static final String FIELD_YEAR = "year";
  private static final String FIELD_MONTH = "month";
  private static final String FIELD_DAY = "day";
  private static final String FIELD_HOUR = "hour";
  private static final String FIELD_MINUTE = "minute";

  public static final Partitioning PARTITIONING = Partitioning.builder()
    .addIntField(FIELD_YEAR)
    .addIntField(FIELD_MONTH)
    .addIntField(FIELD_DAY)
    .addIntField(FIELD_HOUR)
    .addIntField(FIELD_MINUTE)
    .build();

  // for testing: if arguments contain this key, dataset behaves as if it was created pre-2.8
  @VisibleForTesting
  public static final String ARGUMENT_LEGACY_DATASET = "legacy.dataset";

  // this will help us distinguish legacy (2.7) partitions, because they do not have this column
  static final byte[] YEAR_COLUMN_KEY = Bytes.add(FIELD_PREFIX, Bytes.toBytes(FIELD_YEAR));

  // this flag will tell whether the dataset was created before release 2.8
  private final boolean isLegacyDataset;

  public TimePartitionedFileSetDataset(CConfiguration cConf, String name,
                                       FileSet fileSet, Table partitionTable,
                                       DatasetSpecification spec, Map<String, String> arguments,
                                       Provider<ExploreFacade> exploreFacadeProvider) {
    super(cConf, name, PARTITIONING, fileSet, partitionTable, spec, arguments, exploreFacadeProvider);

    isLegacyDataset = arguments.containsKey(ARGUMENT_LEGACY_DATASET) ||
      PartitionedFileSetProperties.getPartitioning(spec.getProperties()) == null;
    if (isLegacyDataset) {
      // prevents overly verbose logging of legacy row keys
      ignoreInvalidRowsSilently = true;
      LOG.info("Backward compatibility mode for dataset '{}' is turned on.", getName());
    }
  }

  @Override
  public void addPartition(long time, String path) {
    if (isLegacyDataset && getLegacyPartition(time) != null) {
      throw new DataSetException(String.format("Dataset '%s' already has a partition with the same time: %d",
                                               getName(), time));
    }
    addPartition(partitionKeyForTime(time), path);
  }

  @Override
  public void dropPartition(long time) {
    dropPartition(partitionKeyForTime(time));
    if (isLegacyDataset) {
      dropLegacyPartition(time);
    }
  }

  @Override
  public String getPartition(long time) {
    String path = getPartition(partitionKeyForTime(time));
    if (path == null && isLegacyDataset) {
      return getLegacyPartition(time);
    }
    return path;
  }

  @Override
  public Collection<String> getPartitionPaths(long startTime, long endTime) {
    final Set<String> paths = Sets.newHashSet();
    for (PartitionFilter filter : partitionFiltersForTimeRange(startTime, endTime)) {
      paths.addAll(getPartitionPaths(filter));
    }
    if (isLegacyDataset) {
      getLegacyPartitions(startTime, endTime, new PartitionConsumer() {
        @Override
        public void consume(byte[] row, String path) {
          paths.add(path);
        }
      });
    }
    return paths;
  }

  @Override
  public Map<Long, String> getPartitions(long startTime, long endTime) {
    final Map<Long, String> partitions = Maps.newHashMap();
    for (PartitionFilter filter : partitionFiltersForTimeRange(startTime, endTime)) {
      for (Map.Entry<PartitionKey, String> entry : getPartitions(filter).entrySet()) {
        partitions.put(timeForPartitionKey(entry.getKey()), entry.getValue());
      }
    }
    if (isLegacyDataset) {
      getLegacyPartitions(startTime, endTime, new PartitionConsumer() {
        @Override
        public void consume(byte[] row, String path) {
          partitions.put(Bytes.toLong(row), path);
        }
      });
    }
    return partitions;
  }

  @VisibleForTesting
  public void addLegacyPartition(long time, String path) {
    final byte[] rowKey = Bytes.toBytes(time);
    Row row = partitionsTable.get(rowKey);
    if (row != null && !row.isEmpty()) {
      throw new DataSetException(String.format("Dataset '%s' already has a partition with time: %d.",
                                               getName(), time));
    }
    Put put = new Put(rowKey);
    put.add(RELATIVE_PATH, Bytes.toBytes(path));
    partitionsTable.put(put);
    addPartitionToExplore(partitionKeyForTime(time), path);
  }

  /**
   * The pre-2.8 way of getting a partition.
   */
  private String getLegacyPartition(long time) {
    if (!isLegacyDataset) {
      return null;
    }
    final byte[] rowKey = Bytes.toBytes(time);
    Row row = partitionsTable.get(rowKey);
    if (row == null) {
      return null;
    }
    byte[] pathBytes = row.get(RELATIVE_PATH);
    if (pathBytes == null) {
      return null;
    }
    return Bytes.toString(pathBytes);
  }

  /**
   * The pre-2.8 way of dropping a partition.
   */
  private void dropLegacyPartition(long time) {
    final byte[] rowKey = Bytes.toBytes(time);
    partitionsTable.delete(rowKey);
  }

  /**
   * The pre-2.8 way of querying partitions.
   */
  private void getLegacyPartitions(long startTime, long endTime, PartitionConsumer consumer) {

    // the legacy (2.7) implementation of this dataset used the partition time as the row key
    final byte[] startKey = Bytes.toBytes(startTime);
    final byte[] endKey = Bytes.toBytes(endTime);
    Scanner scanner = partitionsTable.scan(startKey, endKey);

    try {
      while (true) {
        Row row = scanner.next();
        if (row == null) {
          break;
        }
        if (!isLegacyPartition(row)) {
          continue;
        }
        byte[] pathBytes = row.get(RELATIVE_PATH);
        if (pathBytes != null) {
          consumer.consume(row.getRow(), Bytes.toString(pathBytes));
        }
      }
    } finally {
      scanner.close();
    }
  }

  private interface PartitionConsumer {
    void consume(byte[] row, String path);
  }

  private static boolean isLegacyPartition(Row row) {
    return
      // legacy rows have a row key that is just the long partition time
      (row.getRow().length == Bytes.SIZEOF_LONG)
        // legacy rows do not have the columns for each partitioning field, especially not for YEAR
        && (row.get(YEAR_COLUMN_KEY) == null);
  }

  @Override
  public Map<String, String> getInputFormatConfiguration() {
    Long startTime = TimePartitionedFileSetArguments.getInputStartTime(getRuntimeArguments());
    Long endTime = TimePartitionedFileSetArguments.getInputEndTime(getRuntimeArguments());
    if (startTime == null && endTime == null) {
      // no times specified, perhaps a partition filter was specified? super will deal with that
      return super.getInputFormatConfiguration();
    }
    if (startTime == null) {
      throw new DataSetException("Start time for input time range must be given as argument.");
    }
    if (endTime == null) {
      throw new DataSetException("End time for input time range must be given as argument.");
    }
    Collection<String> inputPaths = getPartitionPaths(startTime, endTime);
    List<Location> inputLocations = Lists.newArrayListWithExpectedSize(inputPaths.size());
    for (String path : inputPaths) {
      inputLocations.add(files.getLocation(path));
    }
    return files.getInputFormatConfiguration(inputLocations);
  }

  @Override
  @Deprecated
  public FileSet getUnderlyingFileSet() {
    return getEmbeddedFileSet();
  }

  @VisibleForTesting
  static PartitionKey partitionKeyForTime(long time) {
    Calendar calendar = Calendar.getInstance();
    calendar.setTimeInMillis(time);
    int year = calendar.get(Calendar.YEAR);
    int month = calendar.get(Calendar.MONTH) + 1; // otherwise January would be 0
    int day = calendar.get(Calendar.DAY_OF_MONTH);
    int hour = calendar.get(Calendar.HOUR_OF_DAY);
    int minute = calendar.get(Calendar.MINUTE);
    return PartitionKey.builder()
      .addIntField(FIELD_YEAR, year)
      .addIntField(FIELD_MONTH, month)
      .addIntField(FIELD_DAY, day)
      .addIntField(FIELD_HOUR, hour)
      .addIntField(FIELD_MINUTE, minute)
      .build();
  }

  @VisibleForTesting
  static long timeForPartitionKey(PartitionKey key) {
    int year = (Integer) key.getField(FIELD_YEAR);
    int month = (Integer) key.getField(FIELD_MONTH) - 1;
    int day = (Integer) key.getField(FIELD_DAY);
    int hour = (Integer) key.getField(FIELD_HOUR);
    int minute = (Integer) key.getField(FIELD_MINUTE);
    Calendar calendar = Calendar.getInstance();
    calendar.clear();
    calendar.set(year, month, day, hour, minute);
    return calendar.getTimeInMillis();
  }

  // returns a list of partition filters that cover that specified time range.
  // this may return a list with a single null filter (in case the range is unbounded in both directions)
  @VisibleForTesting
  static List<PartitionFilter> partitionFiltersForTimeRange(long startTime, long endTime) {

    // unsatisfiable range
    if (startTime >= endTime) {
      return Collections.emptyList();
    }

    PartitionKey keyLower = startTime <= 0 ? null : partitionKeyForTime(startTime);
    PartitionKey keyUpper = endTime == Long.MAX_VALUE ? null : partitionKeyForTime(endTime);

    // no bounds -> no filter
    if (keyLower == null && keyUpper == null) {
      return Collections.singletonList(null); // no filter needed to select all time
    }

    List<PartitionFilter> filters = Lists.newArrayList();
    String[] allFields = PARTITIONING.getFields().keySet().toArray(new String[PARTITIONING.getFields().size()]);

    // if there is no lower bound, we only need the filters for the upper bound
    if (keyLower == null) {
      addUpperFilters(allFields, 0, keyUpper, filters, initialSupplier());
      return filters;
    }
    // if there is no upper bound, we only need the filters for the lower bound
    if (keyUpper == null) {
      addLowerFilters(allFields, 0, keyLower, filters, initialSupplier());
      return filters;
    }

    return filtersFor(allFields, 0, keyLower, keyUpper, filters, initialSupplier());
  }


  // this generates the filters for a suffix of the fields in the partition key. All filters will be
  // prefixed with the conditions that are generated by the provided partition builder supplier.
  // for example, if fields only contains day, hour and minute, then the supplier will return builders
  // that already have conditions for the year and the month.
  private static List<PartitionFilter> filtersFor(String[] fields, int position,
                                                  PartitionKey keyLower, PartitionKey keyUpper,
                                                  List<PartitionFilter> filters,
                                                  final Supplier<PartitionFilter.Builder> supplier) {
    // examined all fields? -> done, build a filter and return.
    if (position >= fields.length) {
      filters.add(supplier.get().build());
      return filters;
    }
    String fieldName = fields[position];
    int lower = (Integer) keyLower.getField(fieldName);
    int upper = (Integer) keyUpper.getField(fieldName);

    // both upper and lower bound specify the same value for this field.
    // Add an equality constraint for this field and value and continue with the next field
    if (lower == upper) {
      return filtersFor(fields, position + 1, keyLower, keyUpper, filters, nextSupplier(supplier, fieldName, lower));
    }

    // we have two different value. For example, if year and month are already provided by the supplier, we are
    // looking at field "day":
    // - lower bound is year/month/15/h1/m1
    // - upper bound is year/month/20/h2/m2
    // The conditions we need are either one of (with fixed year and month):
    // - day is 15, and hour/minute are greater or equal than h1/m1 (addLowerFilters)
    // - day is in 16...19, (add a condition to the supplier and descend to next field, see few special cases)
    // - day is 20, and hour/minute are less than h2/m2 (addUpperFilters)

    // generate the filters for the lower bound on the next level
    addLowerFilters(fields, position + 1, keyLower, filters, nextSupplier(supplier, fieldName, lower));

    // if this field is at the finest granularity (minutes), we must include its value in the range and finish.
    // for example, lower = y/m/d/h/10, upper = y/m/d/h/15, then we need to add a range condition: minute in [10...15]
    if (fields.length - 1 == position) {
      if (lower + 1 == upper) { // special case: range of size one is a single value
        filters.add(supplier.get().addValueCondition(fieldName, lower).build());
      } else {
        filters.add(supplier.get().addRangeCondition(fieldName, lower, upper).build());
      }
    } else {
      // it is not the minute field: we add a condition for this field and descend. Other than in the
      // minute case, the lower key's value is not included in the range (it was processed by addLowerFilters())
      // if upper == lower + 1, then there are no values between them and no filter is added here.
      if (lower + 2 == upper) {
        filters.add(supplier.get().addValueCondition(fieldName, lower + 1).build());
      } else if (lower + 2 < upper) {
        filters.add(supplier.get().addRangeCondition(fieldName, lower + 1, upper).build());
      }
    }

    // generate the filters for the upper bound on the next level
    return addUpperFilters(fields, position + 1, keyUpper, filters, nextSupplier(supplier, fieldName, upper));
  }

  // adds filters for the lower bound, starting at a given field, with conditions on the higher levels supplied
  private static List<PartitionFilter> addLowerFilters(String[] fields, int position, PartitionKey keyLower,
                                                       List<PartitionFilter> filters,
                                                       Supplier<PartitionFilter.Builder> supplier) {
    if (position >= fields.length) {
      return filters;
    }
    String fieldName = fields[position];
    int lower = (Integer) keyLower.getField(fieldName);

    // if this field is at the finest granularity (minutes), we must include its value in the range
    // otherwise we exclude it from the range and descend into the next finer granularity with a value
    // constraints on the current field name. For example:
    // - for hour:15/minute:10, we add a filter for hour>=16 (excluding 15) and descent for hour=15
    // - now the remaining field is minute:10, we add a filter for minute>=10 (including 10)
    int lowerBound = position == fields.length - 1 ? lower : lower + 1;

    // only add the filter if this condition is satisfiable. For example, not for hour>=24 or month>=13
    if (isSatisfiableLowerBound(fieldName, lowerBound)) {
      filters.add(supplier.get().addRangeCondition(fieldName, lowerBound, null).build());
    }
    return addLowerFilters(fields, position + 1, keyLower, filters, nextSupplier(supplier, fieldName, lower));
  }

  // adds filters for the upper bound, starting at a given field, with conditions on the higher levels supplied
  private static List<PartitionFilter> addUpperFilters(String[] fields, int position, PartitionKey keyUpper,
                                                       List<PartitionFilter> filters,
                                                       Supplier<PartitionFilter.Builder> supplier) {
    if (position >= fields.length) {
      return filters;
    }
    String fieldName = fields[position];
    int upper = (Integer) keyUpper.getField(fieldName);
    // only add the filter if this condition is satisfiable. For example, not for hour<0 or month<1
    if (isSatisfiableUpperBound(fieldName, upper)) {
      filters.add(supplier.get().addRangeCondition(fieldName, null, upper).build());
    }
    return addUpperFilters(fields, position + 1, keyUpper, filters, nextSupplier(supplier, fieldName, upper));
  }

  private static Supplier<PartitionFilter.Builder> initialSupplier() {
    return new Supplier<PartitionFilter.Builder>() {
      @Override
      public PartitionFilter.Builder get() {
        return PartitionFilter.builder();
      }
    };
  }

  private static Supplier<PartitionFilter.Builder> nextSupplier(final Supplier<PartitionFilter.Builder> supplier,
                                                                final String field, final int value) {
    return new Supplier<PartitionFilter.Builder>() {
      @Override
      public PartitionFilter.Builder get() {
        return supplier.get().addValueCondition(field, value);
      }
    };
  }

  // this is not the smartest... for example, some months have less than 31 days. So we will sometimes generate
  // a filter that is no satisfiable. It has no effect because that filter will not match any partitions, but
  // may add a small performance overhead in these cases. It does not seem worth the effort of dealing with the
  // different months - and leap years - to make this accurate.
  private static boolean isSatisfiableLowerBound(String fieldName, int lowerBound) {
    if (fieldName.equals(FIELD_MONTH)) {
      return lowerBound <= 12;
    }
    if (fieldName.equals(FIELD_DAY)) {
      return lowerBound <= 31;
    }
    if (fieldName.equals(FIELD_HOUR)) {
      return lowerBound <= 23;
    }
    if (fieldName.equals(FIELD_MINUTE)) {
      return lowerBound <= 60;
    }
    return true;
  }

  private static boolean isSatisfiableUpperBound(String fieldName, int upperBound) {
    if (fieldName.equals(FIELD_YEAR)) {
      return upperBound > 1; // this could be 1968 because no time is before the epoch. But just to be sure...
    }
    if (fieldName.equals(FIELD_MONTH)) {
      return upperBound > 1;
    }
    if (fieldName.equals(FIELD_DAY)) {
      return upperBound > 1;
    }
    return upperBound > 0;
  }

  /**
   * This can be called to determine whether the dataset was created before 2.8 and needs migration.
   *
   * @return whether this is a legacy dataset.
   */
  public boolean isLegacyDataset() {
    return isLegacyDataset;
  }

  /**
   * Migrate legacy partitions to the current format, starting a the given partition time, spending at most a limited
   * number of seconds. The caller can invoke this repeatedly until it returns -1.
   * <p>
   * This method is not in the API for this dataset. It is implementation-specific. The upgrade tool must obtain an
   * instance of {@link TimePartitionedFileSet} and cast it to this class.
   *
   * @param startTime the partition time to start at
   * @param timeLimitInSeconds the number of seconds after which to stop. This is to avoid transaction timeouts.
   * @return the start time for the next call of this method. All partitions with a lesser time stamp have been
   *     migrated. When there are no more entries to migrate, returns -1.
   */
  public long upgradeLegacyEntries(long startTime, long timeLimitInSeconds) {
    long timeLimit = System.currentTimeMillis() + 1000L * timeLimitInSeconds;
    startTime = Math.max(0L, startTime);
    byte[] startRow = Bytes.toBytes(startTime);
    long partitionTime = startTime;
    Scanner scanner = partitionsTable.scan(startRow, null);
    try {
      while (timeLimit - System.currentTimeMillis() > 0) {
        Row row = scanner.next();
        if (row == null) {
          return -1; // no more legacy entries
        }
        if (!isLegacyPartition(row)) {
          continue;
        }
        partitionTime = Bytes.toLong(row.getRow());
        byte[] pathBytes = row.get(RELATIVE_PATH);
        if (pathBytes != null) {
          String path = Bytes.toString(pathBytes);
          addPartition(partitionKeyForTime(partitionTime), path,
                       false); // do not register in explore - it is already there
          partitionsTable.delete(row.getRow());
        } else {
          LOG.info("Dropping legacy partition for time %d because it has no path.", partitionTime);
          dropPartition(partitionTime);
        }
      }
      return partitionTime + 1; // next scan can start after the last partition processed
    } finally {
      scanner.close();
    }
  }

}
