/*
 * Copyright © 2019 Cask Data, Inc.
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

package io.cdap.cdap.internal.app.runtime.schedule.store;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.gson.reflect.TypeToken;
import io.cdap.cdap.api.ProgramStatus;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.proto.ScheduleDetail;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.DatasetId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.ProgramId;
import io.cdap.cdap.spi.data.StructuredTableContext;
import io.cdap.cdap.spi.data.TableNotFoundException;
import io.cdap.cdap.store.StoreDefinition;
import java.lang.reflect.Type;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.quartz.CronExpression;

/**
 * Common utility methods for scheduling.
 */
public class Schedulers {

  public static final String STORE_TYPE_NAME = ProgramScheduleStoreDataset.class.getSimpleName();
  public static final DatasetId STORE_DATASET_ID = NamespaceId.SYSTEM.dataset("schedule.store");

  public static final Type SCHEDULE_DETAILS_TYPE = new TypeToken<List<ScheduleDetail>>() {
  }.getType();

  public static final long JOB_QUEUE_TIMEOUT_MILLIS = TimeUnit.DAYS.toMillis(1);

  public static final int SUBSCRIBER_TX_TIMEOUT_SECONDS = 30;
  public static final long SUBSCRIBER_TX_TIMEOUT_MILLIS =
      1000 * (long) SUBSCRIBER_TX_TIMEOUT_SECONDS;

  /**
   * Returns the metrics tags for schedule job metrics emitted while checking and launching jobs.
   *
   * @param application the application of the schedule
   * @param schedule the name of the schedule
   * @return the metrics tags
   */
  public static Map<String, String> getScheduleJobMetricsTags(String application, String schedule) {
    return ImmutableMap.of(
        Constants.Metrics.Tag.NAMESPACE, NamespaceId.SYSTEM.getEntityName(),
        Constants.Metrics.Tag.APP, application,
        Constants.Metrics.Tag.COMPONENT, "constraintchecker",
        Constants.Metrics.Tag.SCHEDULE, schedule);
  }

  public static String triggerKeyForPartition(DatasetId datasetId) {
    return "partition:" + datasetId.getNamespace() + '.' + datasetId.getDataset();
  }

  public static String triggerKeyForProgramStatus(ProgramId programId,
      ProgramStatus programStatus) {
    return String.format("programStatus:program:%s.%s.%s.%s.%s.%s", programId.getNamespace(),
        programId.getApplication(), ApplicationId.DEFAULT_VERSION,
        programId.getType().getPrettyName().toLowerCase(),
        programId.getProgram(), programStatus.toString().toLowerCase());
  }

  public static Set<String> triggerKeysForProgramStatuses(ProgramId programId,
      Set<ProgramStatus> programStatuses) {
    ImmutableSet.Builder<String> triggerKeysBuilder = ImmutableSet.builder();
    for (ProgramStatus status : programStatuses) {
      triggerKeysBuilder.add(triggerKeyForProgramStatus(programId, status));
    }
    return triggerKeysBuilder.build();
  }

  public static ProgramScheduleStoreDataset getScheduleStore(StructuredTableContext context) {
    try {
      return new ProgramScheduleStoreDataset(
          context.getTable(StoreDefinition.ProgramScheduleStore.PROGRAM_SCHEDULE_TABLE),
          context.getTable(StoreDefinition.ProgramScheduleStore.PROGRAM_TRIGGER_TABLE)
      );
    } catch (TableNotFoundException e) {
      throw Throwables.propagate(e);
    }
  }

  public static void validateCronExpression(String cronExpression) {
    String quartzCron = getQuartzCronExpression(cronExpression);
    try {
      CronExpression.validateExpression(quartzCron);
    } catch (ParseException e) {
      throw new IllegalArgumentException(
          String.format("Invalid cron expression '%s': %s", cronExpression,
              e.getMessage()), e);
    }
  }

  // Maximum valid increment per Quartz cron field: sec, min, hour, day-of-month, month, day-of-week.
  private static final int[] CRON_FIELD_LIMITS = {59, 59, 23, 31, 12, 7};
  private static final String[] CRON_FIELD_MIN_VALUES = {"0", "0", "0", "1", "1", "1"};
  private static final Pattern CRON_STEP_PATTERN = Pattern.compile("^(\\*|\\d+)(?:-\\d+)?/(\\d+)$");

  // Helper function to adapt cron entry to a cronExpression that is usable by Quartz with the following rules:
  // 1. Quartz doesn't support specifying both day-of-the-month and day-of-the-week,
  //    one of them must be '?' if the other is specified with anything other than '?'
  // 2. Quartz resolution is in seconds which cron entry doesn't support.
  // 3. Quartz 2.3+ rejects step increments larger than a field's range (e.g. 0/60, 0/24) that
  //    Quartz 2.2 accepted; normalize those to their equivalent single value.
  public static String getQuartzCronExpression(String cronEntry) {
    // Checks if the cronEntry is quartz cron Expression or unix like cronEntry format.
    // CronExpression will directly be used for tests.
    String parts[] = cronEntry.split(" ");
    Preconditions.checkArgument(parts.length == 5 || parts.length == 6,
        "Invalid cron entry format in '%s'. "
            + "Cron entry must contain 5 or 6 fields.", cronEntry);
    String quartzCron;
    if (parts.length == 5) {
      // Convert cron entry format to Quartz format by replacing wild-card character "*"
      // if day-of-the-month is not "?" and day-of-the-week is wild-card, replace day-of-the-week with "?"
      if (!parts[2].equals("?") && parts[4].equals("*")) {
        parts[4] = "?";
      }
      // if day-of-the-week is not "?" and day-of-the-month is wild-card, replace day-of-the-month with "?"
      if (!parts[4].equals("?") && parts[2].equals("*")) {
        parts[2] = "?";
      }
      List<String> partsList = new ArrayList<>(Arrays.asList(parts));
      // add "0" to seconds in Quartz format without changing the meaning of the original cron expression
      partsList.add(0, "0");
      quartzCron = Joiner.on(" ").join(partsList);
    } else {
      // Use the given cronExpression
      quartzCron = cronEntry;
    }
    return normalizeQuartzCronExpression(quartzCron);
  }

  /**
   * Normalizes legacy Quartz 2.2 cron expressions whose step increment exceeds the field's upper
   * bound (for example {@code 0/60} in minutes or {@code 0/24} in hours). Quartz 2.2 accepted these
   * and fired once per cycle at the starting value; Quartz 2.3+ rejects them with a
   * {@link ParseException}. Rewriting {@code a/n} or {@code a-b/n} (and {@code *&#47;n}) with
   * {@code n > fieldMax} to {@code a} (or the field's minimum value for {@code *}) preserves
   * identical fire times while keeping expressions valid under Quartz 2.3+.
   */
  public static String normalizeQuartzCronExpression(String quartzCron) {
    String[] fields = quartzCron.trim().split("\\s+");
    boolean changed = false;
    for (int i = 0; i < fields.length && i < CRON_FIELD_LIMITS.length; i++) {
      String[] items = fields[i].split(",", -1);
      boolean fieldChanged = false;
      for (int j = 0; j < items.length; j++) {
        Matcher matcher = CRON_STEP_PATTERN.matcher(items[j]);
        if (matcher.matches()) {
          try {
            long step = Long.parseLong(matcher.group(2));
            if (step > CRON_FIELD_LIMITS[i]) {
              String start = matcher.group(1);
              items[j] = "*".equals(start) ? CRON_FIELD_MIN_VALUES[i] : start;
              fieldChanged = true;
            }
          } catch (NumberFormatException ignored) {
            // Leave out-of-range numbers untouched so Quartz validation reports the error.
          }
        }
      }
      if (fieldChanged) {
        fields[i] = String.join(",", items);
        changed = true;
      }
    }
    return changed ? String.join(" ", fields) : quartzCron;
  }
}
