/*
 * Copyright © 2015 Cask Data, Inc.
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

package co.cask.cdap.cli.command;

import co.cask.cdap.api.annotation.Beta;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.cli.ArgumentName;
import co.cask.cdap.cli.CLIConfig;
import co.cask.cdap.cli.ElementType;
import co.cask.cdap.cli.util.AbstractCommand;
import co.cask.cdap.client.QueryClient;
import co.cask.cdap.client.StreamClient;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.explore.client.ExploreExecutionResult;
import co.cask.cdap.proto.ColumnDesc;
import co.cask.cdap.proto.QueryResult;
import co.cask.cdap.proto.StreamProperties;
import co.cask.common.cli.Arguments;
import com.google.common.base.Strings;
import com.google.common.collect.HashMultiset;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multiset;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.inject.Inject;

import java.io.PrintStream;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * A CLI command for getting statistics about stream events.
 */
@Beta
public class GetStreamStatsCommand extends AbstractCommand {

  private static final int DEFAULT_LIMIT = 100;
  private static final int MAX_LIMIT = 100000;

  private final StreamClient streamClient;
  private final QueryClient queryClient;

  @Inject
  public GetStreamStatsCommand(StreamClient streamClient, QueryClient queryClient, CLIConfig cliConfig) {
    super(cliConfig);
    this.streamClient = streamClient;
    this.queryClient = queryClient;
  }

  @Override
  public void perform(Arguments arguments, PrintStream output) throws Exception {
    long currentTime = System.currentTimeMillis();

    String streamId = arguments.get(ArgumentName.STREAM.toString());
    // limit limit to [1, MAX_LIMIT]
    int limit = Math.max(1, Math.min(MAX_LIMIT, arguments.getInt(ArgumentName.LIMIT.toString(), DEFAULT_LIMIT)));
    long startTime = getTimestamp(arguments.get(ArgumentName.START_TIME.toString(), "min"), currentTime);
    long endTime = getTimestamp(arguments.get(ArgumentName.END_TIME.toString(), "max"), currentTime);

    // hack to validate streamId
    StreamProperties config = streamClient.getConfig(streamId);
    if (config.getFormat().getName().equals("text")) {
      output.printf("No schema found for Stream '%s'", streamId);
      output.println();
      return;
    }

    output.printf("Analyzing %d Stream events in the time range [%d, %d]...", limit, startTime, endTime);
    output.println();
    output.println();

    // build processorMap: Hive column name -> StatsProcessor
    Map<String, Set<StatsProcessor>> processorMap = Maps.newHashMap();
    Schema streamSchema = config.getFormat().getSchema();
    for (Schema.Field field : streamSchema.getFields()) {
      Schema fieldSchema = field.getSchema();
      String hiveColumnName = cdapSchemaColumName2HiveColumnName(streamId, field.getName());
      processorMap.put(hiveColumnName, getProcessorsForType(fieldSchema.getType(), fieldSchema.getUnionSchemas()));
    }

    // get a list of stream events and calculates various statistics about the events
    String timestampCol = getTimestampHiveColumn(streamId);
    ListenableFuture<ExploreExecutionResult> resultsFuture = queryClient.execute(
      "SELECT * FROM " + getHiveTableName(streamId)
        + " WHERE " + timestampCol + " BETWEEN " + startTime + " AND " + endTime
        + " LIMIT " + limit);
    ExploreExecutionResult results = resultsFuture.get(1, TimeUnit.MINUTES);
    List<ColumnDesc> schema = results.getResultSchema();

    // apply StatsProcessors to every element in every row
    while (results.hasNext()) {
      QueryResult row = results.next();
      for (int i = 0; i < row.getColumns().size(); i++) {
        Object column = row.getColumns().get(i);
        ColumnDesc columnDesc = schema.get(i);
        String columnName = columnDesc.getName();
        if (isUserHiveColumn(streamId, columnName)) {
          Set<StatsProcessor> processors = processorMap.get(columnName);
          if (processors != null) {
            for (StatsProcessor processor : processors) {
              processor.process(column);
            }
          }
        }
      }
    }

    // print report
    for (ColumnDesc columnDesc : schema) {
      if (isUserHiveColumn(streamId, columnDesc.getName())) {
        String truncatedColumnName = getTruncatedColumnName(streamId, columnDesc.getName());
        output.printf("column: %s, type: %s", truncatedColumnName, columnDesc.getType());
        output.println();
        Set<StatsProcessor> processors = processorMap.get(columnDesc.getName());
        if (processors != null && !processors.isEmpty()) {
          for (StatsProcessor processor : processors) {
            processor.printReport(output);
          }
          output.println();
        } else {
          output.println("No statistics available");
          output.println();
        }
      }
    }
  }

  private String getTruncatedColumnName(String streamId, String hiveColumnName) {
    String hiveTableName = getHiveTableName(streamId);
    String hiveTablePrefix = hiveTableName + ".";
    if (hiveColumnName.startsWith(hiveTablePrefix)) {
      return hiveColumnName.substring(hiveTablePrefix.length());
    }
    return hiveColumnName;
  }

  private String getTimestampHiveColumn(String streamId) {
    return cdapSchemaColumName2HiveColumnName(streamId, "ts");
  }

  private boolean isUserHiveColumn(String streamId, String hiveColumName) {
    // TODO: hardcoded
    return !cdapSchemaColumName2HiveColumnName(streamId, "ts").equals(hiveColumName)
      && !cdapSchemaColumName2HiveColumnName(streamId, "headers").equals(hiveColumName);
  }

  private String getHiveTableName(String streamId) {
    return String.format("cdap_stream_%s_%s", Constants.DEFAULT_NAMESPACE, streamId);
  }

  private String cdapSchemaColumName2HiveColumnName(String streamId, String schemaColumName) {
    return (getHiveTableName(streamId) + "." + schemaColumName).toLowerCase();
  }

  private Set<StatsProcessor> getProcessorsForType(Schema.Type type, List<Schema> unionSchemas) {
    ImmutableSet.Builder<StatsProcessor> result = ImmutableSet.builder();

    boolean isBoolean = isTypeOrInUnion(Schema.Type.DOUBLE, type, unionSchemas);
    boolean isInt = isTypeOrInUnion(Schema.Type.INT, type, unionSchemas);
    boolean isLong = isTypeOrInUnion(Schema.Type.LONG, type, unionSchemas);
    boolean isFloat = isTypeOrInUnion(Schema.Type.FLOAT, type, unionSchemas);
    boolean isDouble = isTypeOrInUnion(Schema.Type.DOUBLE, type, unionSchemas);
    boolean isBytes = isTypeOrInUnion(Schema.Type.DOUBLE, type, unionSchemas);
    boolean isString = isTypeOrInUnion(Schema.Type.STRING, type, unionSchemas);

    if (isBoolean || isInt || isLong || isString || isFloat || isDouble || isBytes) {
      result.add(new CountUniqueProcessor());
    }

    if (isInt || isLong || isFloat || isDouble) {
      result.add(new HistogramProcessor());
    }

    return result.build();
  }

  private boolean isTypeOrInUnion(Schema.Type desiredType, Schema.Type type, List<Schema> unionSchemas) {
    if (desiredType.equals(type)) {
      return true;
    }

    for (Schema unionSchema : unionSchemas) {
      if (desiredType == unionSchema.getType()) {
        return true;
      }
    }

    return false;
  }

  @Override
  public String getPattern() {
    return String.format("get stream-stats <%s> [limit <%s>] [start <%s>] [end <%s>]",
                         ArgumentName.STREAM, ArgumentName.LIMIT, ArgumentName.START_TIME, ArgumentName.END_TIME);
  }

  @Override
  public String getDescription() {
    return "Gets statistics for a " + ElementType.STREAM.getPrettyName() + ". " +
      "The <" + ArgumentName.LIMIT + "> limits how many Stream events to analyze; default is " + DEFAULT_LIMIT + ". " +
      "The time format for <" + ArgumentName.START_TIME + "> and <" + ArgumentName.END_TIME + "> " +
      "can be a timestamp in milliseconds or " +
      "a relative time in the form of [+|-][0-9][d|h|m|s]. " +
      "<" + ArgumentName.START_TIME + "> is relative to current time; " +
      "<" + ArgumentName.END_TIME + ">, it is relative to start time. " +
      "Special constants \"min\" and \"max\" can also be used to represent \"0\" and \"max timestamp\" respectively.";
  }

  /**
   * Processes elements within a Hive column and prints out a report about the elements visited.
   */
  private interface StatsProcessor {
    void process(Object element);
    void printReport(PrintStream printStream);
  }

  /**
   * Reports the number of unique elements found.
   */
  private static final class CountUniqueProcessor implements StatsProcessor {
    private final Set<Object> elements = Sets.newHashSet();

    @Override
    public void process(Object element) {
      if (element != null) {
        elements.add(element);
      }
    }

    @Override
    public void printReport(PrintStream printStream) {
      printStream.print("Unique elements: " + elements.size());
      printStream.println();
    }
  }


  /**
   * Reports a histogram of elements found.
   */
  private static final class HistogramProcessor implements StatsProcessor {

    private static final int MIN_BAR_WIDTH = 5;
    private static final int MAX_PRINT_WIDTH = 80;

    // 0 -> [0, 99], 1 -> [100, 199], etc. (bucket size is BUCKET_SIZE)
    private final Multiset<Integer> buckets = HashMultiset.create();
    private static final int BUCKET_SIZE = 100;

    @Override
    public void process(Object element) {
      if (element != null && element instanceof Number) {
        Number number = (Number) element;
        int bucket = number.intValue() / BUCKET_SIZE;
        buckets.add(bucket);
      }
    }

    @Override
    public void printReport(PrintStream printStream) {
      if (!buckets.isEmpty()) {
        printStream.println("Histogram:");
        List<Integer> sortedBuckets = Lists.newArrayList(buckets.elementSet());
        Collections.sort(sortedBuckets);

        int maxCount = getBiggestBucket().getCount();
        int longestPrefix = getLongestBucketPrefix();
        // max length of the bar
        int maxBarLength = Math.max(MIN_BAR_WIDTH, MAX_PRINT_WIDTH - longestPrefix);

        for (Integer bucketIndex : sortedBuckets) {
          Bucket bucket = new Bucket(bucketIndex, buckets.count(bucketIndex));
          // print padded prefix: e.g. "  [100, 199]: 123    "
          printStream.print(padRight(bucket.getPrefix(), longestPrefix));
          // print the bar: e.g. ============>
          // TODO: determine barLength differently to show difference between 0 and low counts more clearly
          int barLength = (int) ((bucket.getCount() * 1.0 / maxCount) * maxBarLength);
          if (barLength == 0) {
            printStream.print("|");
          } else if (barLength >= 1) {
            printStream.print("|" + Strings.repeat("+", barLength - 1));
          }
          printStream.println();
        }
      }
    }

    private Bucket getBiggestBucket() {
      Bucket biggestBucket = null;
      for (Integer bucketIndex : buckets.elementSet()) {
        Bucket bucket = new Bucket(bucketIndex, buckets.count(bucketIndex));
        if (biggestBucket == null || bucket.getCount() > biggestBucket.getCount()) {
          biggestBucket = bucket;
        }
      }
      return biggestBucket;
    }

    private String padRight(String string, int padding) {
      return String.format("%1$-" + padding + "s", string);
    }

    private int getLongestBucketPrefix() {
      Set<Integer> bucketIndices = buckets.elementSet();
      int longestBucket = Collections.max(bucketIndices, new Comparator<Integer>() {
        @Override
        public int compare(Integer o1, Integer o2) {
          return (Long.toString(o1 * BUCKET_SIZE).length() * 2 + Long.toString(buckets.count(o1)).length())
            - (Long.toString(o2 * BUCKET_SIZE).length() * 2 + Long.toString(buckets.count(o2)).length());
        }
      });
      Bucket bucket = new Bucket(longestBucket, buckets.count(longestBucket));
      String longestBucketPrefix = bucket.getPrefix();
      return longestBucketPrefix.length();
    }

    /**
     *
     */
    private static final class Bucket {
      /**
       * index into {@link GetStreamStatsCommand.HistogramProcessor#buckets}
       */
      private final int index;
      private final int count;

      private Bucket(int index, int count) {
        this.index = index;
        this.count = count;
      }

      public int getIndex() {
        return index;
      }

      public int getCount() {
        return count;
      }

      public int getStartInclusive() {
        return index * BUCKET_SIZE;
      }

      public int getEndInclusive() {
        return getStartInclusive() + (BUCKET_SIZE - 1);
      }

      public String getPrefix() {
        return String.format("  [%d, %d]: %d  ", getStartInclusive(), getEndInclusive(), count);
      }
    }
  }
}
