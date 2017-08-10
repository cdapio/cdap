/*
 * Copyright © 2015-2017 Cask Data, Inc.
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

package co.cask.cdap.logging.gateway.handlers.store;

import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.common.app.RunIds;
import co.cask.cdap.data2.dataset2.lib.table.MDSKey;
import co.cask.cdap.data2.dataset2.lib.table.MetadataStoreDataset;
import co.cask.cdap.internal.app.store.RunRecordMeta;
import co.cask.cdap.proto.id.ApplicationId;
import co.cask.cdap.proto.id.ProgramId;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;

import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Duplicate store class for application meatadata.
 * JIRA https://issues.cask.co/browse/CDAP-2172
 */
public class AppMetadataStore extends MetadataStoreDataset {
  public static final String TYPE_RUN_RECORD_STARTED = "runRecordStarted";
  public static final String TYPE_RUN_RECORD_COMPLETED = "runRecordCompleted";
  public static final String TYPE_RUN_RECORD_SUSPENDED = "runRecordSuspended";

  public AppMetadataStore(Table table) {
    super(table);
  }

  // TODO: getRun is duplicated from cdap-app-fabric AppMetadataStore class.
  // Any changes made here will have to be made over there too.
  // JIRA https://issues.cask.co/browse/CDAP-2172
  public RunRecordMeta getRun(ProgramId program, final String runid) {
    // Query active run record first
    RunRecordMeta running = getUnfinishedRun(program, TYPE_RUN_RECORD_STARTED, runid);
    // If program is running, this will be non-null
    if (running != null) {
      return running;
    }

    // If program is not running, query completed run records
    RunRecordMeta complete = getCompletedRun(program, runid);
    if (complete != null) {
      return complete;
    }

    // Else query suspended run records
    return getUnfinishedRun(program, TYPE_RUN_RECORD_SUSPENDED, runid);
  }

  /**
   * @return run records for runs that do not have start time in mds key for the run record.
   */
  private RunRecordMeta getUnfinishedRun(ProgramId programId, String recordType, String runid) {
    MDSKey runningKey = getProgramKeyBuilder(recordType, programId)
      .add(runid)
      .build();

    RunRecordMeta runRecordMeta = get(runningKey, RunRecordMeta.class);

    if (runRecordMeta == null && programId.getVersion().equals(ApplicationId.DEFAULT_VERSION)) {
      runningKey = getVersionLessProgramKeyBuilder(recordType, programId).add(runid).build();
      return get(runningKey, RunRecordMeta.class);
    }

    return runRecordMeta;
  }

  private RunRecordMeta getCompletedRun(ProgramId programId, final String runid) {
    MDSKey completedKey = getProgramKeyBuilder(TYPE_RUN_RECORD_COMPLETED, programId).build();
    RunRecordMeta runRecordMeta = getCompletedRun(completedKey, runid);

    if (runRecordMeta == null && programId.getVersion().equals(ApplicationId.DEFAULT_VERSION)) {
      completedKey = getVersionLessProgramKeyBuilder(TYPE_RUN_RECORD_COMPLETED, programId).build();
      return getCompletedRun(completedKey, runid);
    }

    return runRecordMeta;
  }

  private RunRecordMeta getCompletedRun(MDSKey completedKey, final String runid) {
    // Get start time from RunId
    long programStartSecs = RunIds.getTime(RunIds.fromString(runid), TimeUnit.SECONDS);
    if (programStartSecs > -1) {
      // If start time is found, run a get
      MDSKey key = new MDSKey.Builder(completedKey)
        .add(getInvertedTsKeyPart(programStartSecs))
        .add(runid)
        .build();

      return get(key, RunRecordMeta.class);
    } else {
      // If start time is not found, scan the table (backwards compatibility when run ids were random UUIDs)
      MDSKey startKey = new MDSKey.Builder(completedKey).add(getInvertedTsScanKeyPart(Long.MAX_VALUE)).build();
      MDSKey stopKey = new MDSKey.Builder(completedKey).add(getInvertedTsScanKeyPart(0)).build();
      List<RunRecordMeta> runRecords =
        list(startKey, stopKey, RunRecordMeta.class, 1,  // Should have only one record for this runid
             new Predicate<RunRecordMeta>() {
               @Override
               public boolean apply(RunRecordMeta input) {
                 return input.getPid().equals(runid);
               }
             });
      return Iterables.getFirst(runRecords, null);
    }
  }

  private long getInvertedTsKeyPart(long endTime) {
    return Long.MAX_VALUE - endTime;
  }

  /**
   * Returns inverted scan key for given time. The scan key needs to be adjusted to maintain the property that
   * start key is inclusive and end key is exclusive on a scan. Since when you invert start key, it becomes end key and
   * vice-versa.
   */
  private long getInvertedTsScanKeyPart(long time) {
    long invertedTsKey = getInvertedTsKeyPart(time);
    return invertedTsKey < Long.MAX_VALUE ? invertedTsKey + 1 : invertedTsKey;
  }
}
