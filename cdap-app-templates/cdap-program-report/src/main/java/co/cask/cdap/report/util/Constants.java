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

/**
 * Constants used by the report generation app and file schema.
 */
public final class Constants {
  public static final String NAMESPACE = "namespace";
  public static final String ARTIFACT_SCOPE = "artifact.scope";
  public static final String ARTIFACT_NAME = "artifact.name";
  public static final String ARTIFACT_VERSION = "artifact.version";
  public static final String APPLICATION_NAME = "application.name";
  public static final String APPLICATION_VERSION = "application.version";
  public static final String PROGRAM = "program";
  public static final String RUN = "run";
  public static final String STATUS = "status";
  public static final String START = "start";
  public static final String RUNNING = "running";
  public static final String END = "end";
  public static final String DURATION = "duration";
  public static final String USER = "user";
  public static final String START_METHOD = "startMethod";
  public static final String RUNTIME_ARGUMENTS = "runtimeArguments";
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
    public static final String COUNT_FILE = "COUNT";
    public static final String SUCCESS_FILE = "_SUCCESS";
  }
}
