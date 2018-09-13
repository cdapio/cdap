/*
 * Copyright © 2018 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0  = the "License"); you may not
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

package co.cask.cdap.report.util;

import java.util.concurrent.TimeUnit;

/**
 * Constants used by the report generation app and file schema.
 */
public final class Constants {
  /**
   * Constants releated to security key encryption
   */
  public static final class Security {
    public static final String ENCRYPTION_ALGORITHM = "AES";
    public static final String KEY_FILE_NAME = "security_key";
    public static final String KEY_FILE_PERMISSION = "700";
    public static final int ENCRYPTION_KEY_BITSIZE = 128;
  }

  /**
   * Constants related to emitting metrics
   */
  public static final class Metrics {
    public static final String SYSTEM_NAMESPACE = "system";
    public static final String RECORDS_PROCESSED_METRIC = "program.status.processed";
    public static final String SYNC_INTERVAL_TIME_MILLIS_METRIC = "run.metadata.last.sync.time.millis";
  }

  public static final String NAMESPACE = "namespace";
  public static final String ARTIFACT_SCOPE = "artifactScope";
  public static final String ARTIFACT_NAME = "artifactName";
  public static final String ARTIFACT_ID = "artifactId";
  public static final String ARTIFACT_VERSION = "artifactVersion";
  public static final String APPLICATION_NAME = "applicationName";
  public static final String APPLICATION_VERSION = "applicationVersion";
  public static final String PROGRAM_TYPE = "programType";
  public static final String PROGRAM = "program";
  public static final String RUN = "run";
  public static final String STATUS = "status";
  public static final String START = "start";
  public static final String RUNNING = "running";
  public static final String END = "end";
  public static final String DURATION = "duration";
  public static final String USER = "user";
  public static final String START_METHOD = "startMethod";
  public static final String RUNTIME_ARGUMENTS = "runtimeArgs";
  public static final String SYSTEM_ARGUMENTS = "systemArgs";
  public static final String NUM_LOG_WARNINGS = "numLogWarnings";
  public static final String NUM_LOG_ERRORS = "numLogErrors";
  public static final String NUM_RECORDS_OUT = "numRecordsOut";
  public static final String TIME = "time";
  public static final String START_INFO = "startInfo";

  /**
   * Constants used as location names for report generation app.
   */
  public static final class LocationName {
    public static final String REPORT_DIR = "reports";
    public static final String SUMMARY = "_SUMMARY";
  }

  public static final String MESSAGE_ID = "messageId";

  /**
   * Notification property fields
   */
  public static final class Notification {
    public static final String PROGRAM_RUN_ID = "programRunId";
    public static final String PROGRAM_STATUS = "programStatus";
    public static final String ARTIFACT_ID = "artifactId";
    public static final String SYSTEM_OVERRIDES = "systemOverrides";
    public static final String USER_OVERRIDES = "userOverrides";
    public static final String PRINCIPAL = "principal";
    public static final String PROGRAM_DESCRIPTOR = "programDescriptor";

    public static final String LOGICAL_START_TIME = "logical.start.time";
    public static final String END_TIME = "endTime";
    public static final String SUSPEND_TIME = "suspendTime";
    public static final String RESUME_TIME = "resumeTime";
    public static final String SCHEDULE_INFO_KEY = "triggeringScheduleInfo";

    /**
     * Program status options
     */
    public static final class Status {
      public static final String STARTING = "STARTING";
      public static final String RUNNING = "RUNNING";
      public static final String KILLED = "KILLED";
      public static final String COMPLETED = "COMPLETED";
      public static final String FAILED = "FAILED";
      public static final String SUSPENDED = "SUSPENDED";
      public static final String RESUMING = "RESUMING";
    }
  }
  /**
   * Constants related to report generation and expiration
   */
  public static final class Report {
    // report files will expire after 48 hours after they are generated
    public static final String DEFAULT_REPORT_EXPIRY_TIME_SECONDS = String.valueOf(TimeUnit.DAYS.toSeconds(2));
    public static final String REPORT_EXPIRY_TIME_SECONDS = "report.expiry.duration.seconds";
  }

  public static final String DISABLE_TMS_SUBSCRIBER_THREAD = "disable.tms.subscriber.thread";
}
