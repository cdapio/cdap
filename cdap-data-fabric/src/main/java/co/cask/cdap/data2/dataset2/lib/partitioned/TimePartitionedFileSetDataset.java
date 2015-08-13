/*
 * Copyright © 2014-2015 Cask Data, Inc.
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

import co.cask.cdap.api.dataset.DataSetException;
import co.cask.cdap.api.dataset.DatasetContext;
import co.cask.cdap.api.dataset.DatasetSpecification;
import co.cask.cdap.api.dataset.lib.FileSet;
import co.cask.cdap.api.dataset.lib.IndexedTable;
import co.cask.cdap.api.dataset.lib.PartitionDetail;
import co.cask.cdap.api.dataset.lib.PartitionFilter;
import co.cask.cdap.api.dataset.lib.PartitionKey;
import co.cask.cdap.api.dataset.lib.PartitionMetadata;
import co.cask.cdap.api.dataset.lib.PartitionedFileSetProperties;
import co.cask.cdap.api.dataset.lib.Partitioning;
import co.cask.cdap.api.dataset.lib.TimePartitionDetail;
import co.cask.cdap.api.dataset.lib.TimePartitionOutput;
import co.cask.cdap.api.dataset.lib.TimePartitionedFileSet;
import co.cask.cdap.api.dataset.lib.TimePartitionedFileSetArguments;
import co.cask.cdap.explore.client.ExploreFacade;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Supplier;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.inject.Provider;

import java.util.Calendar;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;

/**
 * Implementation of partitioned datasets using a Table to store the meta data.
 */
public class TimePartitionedFileSetDataset extends PartitionedFileSetDataset implements TimePartitionedFileSet {

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

  public TimePartitionedFileSetDataset(DatasetContext datasetContext, String name,
                                       FileSet fileSet, IndexedTable partitionTable,
                                       DatasetSpecification spec, Map<String, String> arguments,
                                       Provider<ExploreFacade> exploreFacadeProvider) {
    super(datasetContext, name, PARTITIONING, fileSet, partitionTable, spec, arguments,
          exploreFacadeProvider);

    // the first version of TPFS in CDAP 2.7 did not have the partitioning in the properties. It is not supported.
    if (PartitionedFileSetProperties.getPartitioning(spec.getProperties()) == null) {
      throw new DataSetException("Unsupported version of TimePartitionedFileSet. Dataset '" + name + "' is missing " +
                                   "the partitioning property. This probably means that it was created in CDAP 2.7, " +
                                   "which is not supported any longer.");
    }
  }

  @Override
  public void addPartition(long time, String path) {
    addPartition(time, path, Collections.<String, String>emptyMap());
  }

  @Override
  public void addPartition(long time, String path, Map<String, String> metadata) {
    addPartition(partitionKeyForTime(time), path, metadata);
  }

  @Override
  public void addMetadata(long time, String metadataKey, String metadataValue) {
    addMetadata(partitionKeyForTime(time), metadataKey, metadataValue);
  }

  @Override
  public void addMetadata(long time, Map<String, String> metadata) {
    addMetadata(partitionKeyForTime(time), metadata);
  }

  @Override
  public void dropPartition(long time) {
    dropPartition(partitionKeyForTime(time));
  }

  @Nullable
  @Override
  public TimePartitionDetail getPartitionByTime(long time) {
    PartitionDetail partitionDetail = getPartition(partitionKeyForTime(time));
    return partitionDetail == null ? null
      : new BasicTimePartitionDetail(this, partitionDetail.getRelativePath(), partitionDetail.getPartitionKey(),
                                     partitionDetail.getMetadata());
  }

  @Override
  public Set<TimePartitionDetail> getPartitionsByTime(long startTime, long endTime) {
    final Set<TimePartitionDetail> partitions = Sets.newHashSet();
    for (PartitionFilter filter : partitionFiltersForTimeRange(startTime, endTime)) {
      super.getPartitions(filter, new PartitionedFileSetDataset.PartitionConsumer() {
        @Override
        public void consume(PartitionKey key, String path, @Nullable PartitionMetadata metadata) {
          partitions.add(new BasicTimePartitionDetail(TimePartitionedFileSetDataset.this, path, key, metadata));
        }
      });
    }
    return partitions;
  }

  private Collection<String> getPartitionPathsByTime(long startTime, long endTime) {
    final Set<String> paths = Sets.newHashSet();
    for (PartitionFilter filter : partitionFiltersForTimeRange(startTime, endTime)) {
      super.getPartitions(filter, new PartitionedFileSetDataset.PartitionConsumer() {
        @Override
        public void consume(PartitionKey key, String path, @Nullable PartitionMetadata metadata) {
          paths.add(path);
        }
      });
    }
    return paths;
  }

  @Override
  public TimePartitionOutput getPartitionOutput(long time) {
    if (isExternal) {
      throw new UnsupportedOperationException(
        "Output is not supported for external time-partitioned file set '" + spec.getName() + "'");
    }
    PartitionKey key = partitionKeyForTime(time);
    return new BasicTimePartitionOutput(this, getOutputPath(partitioning, key), key);
  }

  @Override
  @Nullable
  protected Collection<String> computeFilterInputPaths() {
    Long startTime = TimePartitionedFileSetArguments.getInputStartTime(getRuntimeArguments());
    Long endTime = TimePartitionedFileSetArguments.getInputEndTime(getRuntimeArguments());
    if (startTime == null && endTime == null) {
      // no times specified; perhaps a partition filter was specified. super will deal with that
      return super.computeFilterInputPaths();
    }
    if (startTime == null) {
      throw new DataSetException("Start time for input time range must be given as argument.");
    }
    if (endTime == null) {
      throw new DataSetException("End time for input time range must be given as argument.");
    }
    return getPartitionPathsByTime(startTime, endTime);
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
    //noinspection MagicConstant
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

  private static class BasicTimePartitionDetail extends BasicPartitionDetail implements TimePartitionDetail {

    private final Long time;

    private BasicTimePartitionDetail(TimePartitionedFileSetDataset timePartitionedFileSetDataset, String relativePath,
                                     PartitionKey key, PartitionMetadata metadata) {
      super(timePartitionedFileSetDataset, relativePath, key, metadata);
      this.time = timeForPartitionKey(key);
    }

    @Override
    public long getTime() {
      return time;
    }
  }

  private static class BasicTimePartitionOutput extends BasicPartitionOutput implements TimePartitionOutput {

    private final Long time;

    private BasicTimePartitionOutput(TimePartitionedFileSetDataset timePartitionedFileSetDataset, String relativePath,
                                     PartitionKey key) {
      super(timePartitionedFileSetDataset, relativePath, key);
      this.time = timeForPartitionKey(key);
    }

    @Override
    public long getTime() {
      return time;
    }
  }

}
