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

package data.quality.app;

import co.cask.cdap.api.Config;
import co.cask.cdap.api.ProgramLifecycle;
import co.cask.cdap.api.annotation.Property;
import co.cask.cdap.api.app.AbstractApplication;
import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.data.format.Formats;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.data.stream.Stream;
import co.cask.cdap.api.dataset.table.Put;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.api.mapreduce.AbstractMapReduce;
import co.cask.cdap.api.mapreduce.MapReduceConfigurer;
import co.cask.cdap.api.mapreduce.MapReduceContext;
import co.cask.cdap.api.schedule.Schedules;
import co.cask.cdap.api.stream.GenericStreamEventData;

import co.cask.cdap.api.templates.plugins.PluginProperties;
import co.cask.cdap.template.etl.api.batch.BatchSource;
import co.cask.cdap.template.etl.api.batch.BatchSourceContext;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import data.quality.app.etl.MapReduceSourceContext;
import data.quality.app.functions.BasicAggregationFunction;
import data.quality.app.functions.CombinableAggregationFunction;
import data.quality.app.functions.DiscreteValuesHistogram;
import data.quality.app.rowkey.AggregationsRowKey;
import data.quality.app.rowkey.ValuesRowKey;

import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;

/**
 * Data Quality Application
 */
public class DataQualityApp extends AbstractApplication<DataQualityApp.ConfigClass> {
  private static final Logger LOG = LoggerFactory.getLogger(DataQualityApp.class);
  private static final Gson GSON = new Gson();
  private static final Type TOKEN_TYPE_MAP_STRING_SET_STRING =
    new TypeToken<Map<String, Set<String>>>() { }.getType();

  public static final String DEFAULT_AGGREGATION_NAME = DiscreteValuesHistogram.class.getSimpleName();
  public static final int DEFAULT_WORKFLOW_SCHEDULE_MINUTES = 5;
  public static final String DEFAULT_SOURCE_ID = "logStream";
  public static final String DEFAULT_DATASET_NAME = "dataQuality";
  public static final String DEFAULT_INPUT_FORMAT = Formats.COMBINED_LOG_FORMAT;
  public static final Map<String, Set<String>> DEFAULT_MAP = new HashMap<>();

  /**
   * Configuration Class for the application
   * Sets following fields: aggregationName, fields (a comma separated list
   * of the fields we want to aggregate over), workflowScheduleMinutes, sourceID, datasetName,
   * inputFormat, and schema.
   */
  public static class ConfigClass extends Config {
    private String aggregationName;
    private int workflowScheduleMinutes;
    private String sourceID;
    private String datasetName;
    private String inputFormat;
    private String schema;
    private Map<String, Set<String>> fieldAggregations;

    public ConfigClass() {
      this.aggregationName = DEFAULT_AGGREGATION_NAME;
      this.workflowScheduleMinutes = DEFAULT_WORKFLOW_SCHEDULE_MINUTES;
      this.sourceID = DEFAULT_SOURCE_ID;
      this.datasetName = DEFAULT_DATASET_NAME;
      this.inputFormat = DEFAULT_INPUT_FORMAT;
      this.fieldAggregations = DEFAULT_MAP;
    }

    public ConfigClass(int workflowScheduleMinutes, String sourceID,
                       String datasetName, String inputFormat, String schema,
                       Map<String, Set<String>> fieldAggregations) {
      Preconditions.checkNotNull(workflowScheduleMinutes);
      Preconditions.checkNotNull(sourceID);
      this.aggregationName = DEFAULT_AGGREGATION_NAME;
      this.workflowScheduleMinutes = workflowScheduleMinutes;
      this.sourceID = sourceID;
      this.datasetName = datasetName;
      this.inputFormat = inputFormat;
      this.schema = schema;
      this.fieldAggregations = fieldAggregations;
    }
  }

  @Override
  public void configure() {
    ConfigClass configObj = getContext().getConfig();
    Integer scheduleMinutes = configObj.workflowScheduleMinutes;
    addMapReduce(new FieldAggregator(configObj.aggregationName,
                                     configObj.workflowScheduleMinutes,
                                     configObj.sourceID,
                                     configObj.datasetName,
                                     configObj.inputFormat,
                                     configObj.schema,
                                     configObj.fieldAggregations));
    setName("DataQualityApp");
    setDescription("Application with MapReduce job using stream as input");
    addStream(new Stream(configObj.sourceID));
    createDataset(configObj.datasetName, Table.class);
    addService(new AggregationsService(configObj.datasetName));
    addWorkflow(new DataQualityWorkflow());
    String schedule = "*/" + scheduleMinutes + " * * * *";
    scheduleWorkflow(Schedules.createTimeSchedule("Data Quality Workflow Schedule",
                                                  "Schedule execution every" + scheduleMinutes
                                                    + " min", schedule), "DataQualityWorkflow");
  }

  /**
   * Map Reduce job that ingests a stream of data and builds an aggregation of the ingested data
   * that maps timestamp, sourceID, field type and field value to frequency.
   */
  public static final class FieldAggregator extends AbstractMapReduce {

    @Property
    private final String aggregationName;

    @Property
    private final int workflowScheduleMinutes;

    @Property
    private final String sourceID;

    @Property
    private final String datasetName;

    @Property
    private final String inputFormat;

    @Property
    private final String schema;

    private final Map<String, Set<String>> fieldAggregations;

    BatchSource batchSource;

    public FieldAggregator(String aggregationName, int workflowScheduleMinutes,
                           String sourceID, String datasetName, String inputFormat,
                           String schema, Map<String, Set<String>> fieldAggregations) {
      this.aggregationName = aggregationName;
      this.workflowScheduleMinutes = workflowScheduleMinutes;
      this.sourceID = sourceID;
      this.datasetName = datasetName;
      this.inputFormat = inputFormat;
      this.schema = schema;
      this.fieldAggregations = fieldAggregations;
    }

    @Override
    public void configure() {
      super.configure();
      MapReduceConfigurer mrc = getConfigurer();
      mrc.usePlugin("source", "Stream", sourceID,
                                PluginProperties.builder()
                                  .add("name", sourceID)
                                  .add("duration", String.valueOf(workflowScheduleMinutes) + "m")
                                  .add("delay", "0m")
                                  .add("format", inputFormat)
                                  .add("schema", schema)
                                  .build());
      setName("FieldAggregator");
      setOutputDataset(datasetName);
      setProperties(ImmutableMap.<String, String>builder()
                      .put("agg", aggregationName)
                      .put("workflowScheduleMinutes", Integer.toString(workflowScheduleMinutes))
                      .put("sourceID", sourceID)
                      .put("inputFormat", inputFormat)
                      .put("schema", schema == null ? "" : schema) // Need to do this because we cannot pass
                                                                   // null arguments to the ImmutableMap
                      .put("fieldAggregations", GSON.toJson(fieldAggregations))
                      .build());
    }

    @Override
    public void beforeSubmit(MapReduceContext context) throws Exception {
      Job job = context.getHadoopJob();
      job.setMapperClass(AggregationMapper.class);
      job.setReducerClass(AggregationReducer.class);
      batchSource = context.newInstance(context.getSpecification().getProperties().get("sourceID"));
      BatchSourceContext sourceContext = new MapReduceSourceContext(context, null,
                                                                    context.getSpecification().
                                                                      getProperties().get("sourceID"));
      batchSource.prepareRun(sourceContext);
    }
  }

  /**
   * Take in data and output a field type as a key and a DataQualityWritable containing the field value
   * as an associated value.
   */
  public static class AggregationMapper extends
    Mapper<LongWritable, GenericStreamEventData<StructuredRecord>, Text, DataQualityWritable>
    implements ProgramLifecycle<MapReduceContext> {
    private Set<String> fieldsSet = new HashSet<>();

    @Override
    public void map(LongWritable key, GenericStreamEventData<StructuredRecord> event, Context context)
      throws IOException, InterruptedException {
      StructuredRecord body = event.getBody();
      for (Schema.Field field : body.getSchema().getFields()) {
        String fieldName = field.getName();
        Object fieldVal = body.get(fieldName);
        if (fieldVal != null) {
          DataQualityWritable outputValue;
          if (field.getSchema().isNullable()) {
            outputValue = getOutputValue(field.getSchema().getNonNullable().getType(), fieldVal);
          } else {
            outputValue = getOutputValue(field.getSchema().getType(), fieldVal);
          }
          if (outputValue != null && (fieldsSet.contains(fieldName) || fieldsSet.isEmpty())) {
            context.write(new Text(fieldName), outputValue);
          }
        }
      }
    }

    @Nullable
    private DataQualityWritable getOutputValue(Schema.Type type, Object o) {
      DataQualityWritable writable = new DataQualityWritable();
      switch (type) {
        case STRING:
          writable.set(new Text((String) o));
          break;
        case INT:
          writable.set(new IntWritable((Integer) o));
          break;
        case LONG:
          writable.set(new LongWritable((Long) o));
          break;
        case DOUBLE:
          writable.set(new DoubleWritable((Double) o));
          break;
        case FLOAT:
          writable.set(new FloatWritable((Float) o));
          break;
        case BOOLEAN:
          writable.set(new BooleanWritable((Boolean) o));
          break;
        case BYTES:
          writable.set(new ByteWritable((Byte) o));
          break;
        default:
          return null;
      }
      return writable;
    }

    @Override
    public void initialize(MapReduceContext mapReduceContext) throws Exception {
      Map<String, Set<String>> fieldAggregations =
        GSON.fromJson(mapReduceContext.getSpecification().getProperties().get("fieldAggregations"),
                      TOKEN_TYPE_MAP_STRING_SET_STRING);
      fieldsSet = fieldAggregations.keySet();
    }

    @Override
    public void destroy() {
    }
  }

  /**
   * Generate and write an aggregation with the data collected by the mapper
   */
  public static class AggregationReducer extends Reducer<Text, DataQualityWritable, byte[], Put>
    implements ProgramLifecycle<MapReduceContext> {
    private static final Gson GSON = new Gson();
    private String sourceID;
    private Map<String, Set<String>> fieldAggregations;
    private String defaultAggregationType;
    long timeKey = 0;

    @Override
    public void reduce(Text key, Iterable<DataQualityWritable> values, Context context)
      throws IOException, InterruptedException {
      LOG.debug("timestamp: {}", timeKey);
      Set<String> aggregationTypesSet = fieldAggregations.get(key.toString());
      if (aggregationTypesSet == null) {
        aggregationTypesSet = new HashSet<>();
        aggregationTypesSet.add(defaultAggregationType);
      }
      List<AggregationTypeValue> aggregationTypeValueList = new ArrayList<>();
      AggregationsRowKey aggregationsRowKey = new AggregationsRowKey(timeKey, sourceID);
      byte[] fieldColumnKey = Bytes.toBytes(key.toString());
      for (String aggregationType : aggregationTypesSet) {
        boolean isCombinable = true;
        try {
          Class<?> aggregationClass = Class.forName("data.quality.app.functions." + aggregationType);
          BasicAggregationFunction instance = (BasicAggregationFunction) aggregationClass.newInstance();
          isCombinable = instance instanceof CombinableAggregationFunction;
          for (DataQualityWritable value : values) {
            instance.add(value);
          }
          ValuesRowKey valuesRowKey = new ValuesRowKey(timeKey, key.toString(), sourceID);
          context.write(valuesRowKey.getTableRowKey(), new Put(valuesRowKey.getTableRowKey(),
                                                               Bytes.toBytes(aggregationType),
                                                               instance.aggregate()));

          AggregationTypeValue aggregationTypeValue = new AggregationTypeValue(aggregationType, isCombinable);
          aggregationTypeValueList.add(aggregationTypeValue);
        } catch (ClassNotFoundException | RuntimeException | InstantiationException | IllegalAccessException e) {
          throw new RuntimeException(e);
        }
      }
      byte[] aggregationTypeListBytes = Bytes.toBytes(GSON.toJson(aggregationTypeValueList));
      context.write(aggregationsRowKey.getTableRowKey(),
                    new Put(aggregationsRowKey.getTableRowKey(), fieldColumnKey, aggregationTypeListBytes));
      //TODO: There are issues if multiple apps use the same source and same fields, but different aggregations
    }

    @Override
    public void initialize(MapReduceContext mapReduceContext) throws Exception {
      timeKey = mapReduceContext.getLogicalStartTime();
      sourceID = mapReduceContext.getSpecification().getProperties().get("sourceID");
      fieldAggregations = GSON.fromJson(mapReduceContext
                                                  .getSpecification().getProperties().get("fieldAggregations"),
                                                TOKEN_TYPE_MAP_STRING_SET_STRING);
      defaultAggregationType = mapReduceContext.getSpecification().getProperties().get("agg");
    }

    @Override
    public void destroy() {
    }
  }
}
