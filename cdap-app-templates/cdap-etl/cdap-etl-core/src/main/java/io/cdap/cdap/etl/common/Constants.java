/*
 * Copyright © 2015-2016 Cask Data, Inc.
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

package io.cdap.cdap.etl.common;

/**
 * Constants used in ETL Applications.
 */
public final class Constants {
  public static final String ID_SEPARATOR = ":";
  public static final String PIPELINEID = "pipeline";
  public static final String PIPELINE_SPEC_KEY = "pipeline.spec";
  public static final String STAGE_LOGGING_ENABLED = "stage.logging.enabled";
  public static final String EVENT_TYPE_TAG = "MDC:eventType";
  public static final String PIPELINE_LIFECYCLE_TAG_VALUE = "lifecycle";
  public static final String SPARK_PROGRAM_PLUGIN_TYPE = "sparkprogram";
  public static final String CONNECTOR_DATASETS = "connector.datasets";
  public static final String MDC_STAGE_KEY = "pipeline.stage";
  public static final String FIELD_OPERATION_KEY_IN_WORKFLOW_TOKEN = "field.operations";
  public static final String SPARK_PIPELINE_AUTOCACHE_ENABLE_FLAG = "spark.cdap.pipeline.autocache.enable";
  public static final String SPARK_PIPELINE_CACHING_STORAGE_LEVEL = "spark.cdap.pipeline.caching.storage.level";
  public static final String CONSOLIDATE_STAGES = "spark.cdap.pipeline.consolidate.stages";
  public static final String DEFAULT_CACHING_STORAGE_LEVEL = "DISK_ONLY";

  private Constants() {
    throw new AssertionError("Suppress default constructor for noninstantiability");
  }

  /**
   * Connector constants
   */
  public static final class Connector {
    public static final String PLUGIN_TYPE = "connector";
    public static final String ORIGINAL_NAME = "original";
    public static final String TYPE = "type";
    public static final String SOURCE_TYPE = "source";
    public static final String SINK_TYPE = "sink";
    public static final String DATA_DIR = "data";
  }

  /**
   * Various metric constants.
   */
  public static final class Metrics {
    public static final String TOTAL_TIME = "process.time.total";
    public static final String MIN_TIME = "process.time.min";
    public static final String MAX_TIME = "process.time.max";
    public static final String STD_DEV_TIME = "process.time.stddev";
    public static final String AVG_TIME = "process.time.avg";
    public static final String RECORDS_IN = "records.in";
    public static final String RECORDS_OUT = "records.out";
    public static final String RECORDS_ERROR = "records.error";
    public static final String RECORDS_ALERT = "records.alert";
    public static final String AGG_GROUPS = "aggregator.groups";
    public static final String JOIN_KEYS = "joiner.keys";
    public static final String DRAFT_COUNT = "draft.count";
  }

  /**
   * Constants related to the stage statistics.
   */
  public static final class StageStatistics {
    public static final String PREFIX = "stage.statistics";
    public static final String INPUT_RECORDS = "input.records";
    public static final String OUTPUT_RECORDS = "output.records";
    public static final String ERROR_RECORDS = "error.records";
  }
}
