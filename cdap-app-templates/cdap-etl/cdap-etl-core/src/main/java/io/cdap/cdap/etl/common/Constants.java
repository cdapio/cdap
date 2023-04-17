/*
 * Copyright © 2015-2021 Cask Data, Inc.
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
  public static final String STUDIO_SERVICE_NAME = "studio";
  public static final String CONNECTION_SERVICE_NAME = "connection";
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
  public static final String CACHE_FUNCTIONS = "spark.cdap.pipeline.functioncache.enable";
  public static final String DATASET_KRYO_ENABLED = "spark.cdap.pipeline.dataset.kryo.enable";
  public static final String DATASET_AGGREGATE_ENABLED = "spark.cdap.pipeline.aggregate.dataset.enable";
  public static final String DISABLE_ELT_PUSHDOWN = "cdap.pipeline.pushdown.disable";
  public static final String DATASET_AGGREGATE_IGNORE_PARTITIONS =
      "spark.cdap.pipeline.aggregate.dataset.partitions.ignore";
  public static final String DEFAULT_CACHING_STORAGE_LEVEL = "DISK_ONLY";
  // Can be used as a runtime argument for streaming pipeline to disable at least once processing.
  public static final String CDAP_STREAMING_ATLEASTONCE_ENABLED = "cdap.streaming.atleastonce.enabled";
  // Can be used as a runtime argument for streaming pipeline to set max retry time in minutes
  public static final String CDAP_STREAMING_MAX_RETRY_TIME_IN_MINS = "cdap.streaming.maxRetryTimeInMins";
  // Can be used as a runtime argument for streaming pipeline to set base retry delay in seconds
  public static final String CDAP_STREAMING_BASE_RETRY_DELAY_IN_SECONDS = "cdap.streaming.baseRetryDelayInSeconds";
  // Can be used as a runtime argument for streaming pipeline to set max retry delay in seconds
  public static final String CDAP_STREAMING_MAX_RETRY_DELAY_IN_SECONDS = "cdap.streaming.maxRetryDelayInSeconds";
  // Can be used as a runtime argument for streaming pipelines to allow macros in the source,
  // even when using spark checkpointing.
  public static final String CDAP_STREAMING_ALLOW_SOURCE_MACROS = "cdap.streaming.allow.source.macros";

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
    public static final String RECORDS_PUSH = "records.push";
    public static final String RECORDS_PULL = "records.pull";
    public static final String AGG_GROUPS = "aggregator.groups";
    public static final String JOIN_KEYS = "joiner.keys";
    public static final String DRAFT_COUNT = "draft.count";
    public static final String STAGES_COUNT = "stages.count";
    public static final String STAGES_COUNT_PREFIX = STAGES_COUNT + ".";

    public static final class Connection {

      public static final String CONNECTION_COUNT = "connections.count";
      public static final String CONNECTION_DELETED_COUNT = "connections.deleted.count";
      public static final String CONNECTION_GET_COUNT = "connections.get.count";
      public static final String CONNECTION_BROWSE_COUNT = "connections.browse.count";
      public static final String CONNECTION_SAMPLE_COUNT = "connections.sample.count";
      public static final String CONNECTION_SPEC_COUNT = "connections.spec.count";
    }

    /**
     * NOTES: tag names must be unique (keeping all possible here helps to ensure that) tag names
     * better be short to reduce the serialized metric value size
     */
    public static final class Tag {

      //For app entity
      public static final String APP_ENTITY_TYPE = "aet";
      public static final String APP_ENTITY_TYPE_NAME = "tpe";
    }

    /**
     * Metric constants for different modes of DataStreamsStateSpec
     */
    public static final class AtleastOnceProcessing {

      public static final String STREAMING_ATLEASTONCE_DISABLED_COUNT = "streaming.atleastonce.disabled.count";
      public static final String STREAMING_ATLEASTONCE_CHECKPOINTING_COUNT =
          "streaming.atleastonce.checkpointing.count";
      public static final String STREAMING_ATLEASTONCE_STORE_COUNT = "streaming.atleastonce.store.count";
    }

    public static final String STREAMING_MULTI_SOURCE_PIPELINE_RUNS_COUNT =
        "streaming.multi.source.pipeline.runs.count";
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
