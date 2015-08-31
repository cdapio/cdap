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

package co.cask.cdap.dq;

import co.cask.cdap.api.annotation.Property;
import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.dataset.table.Row;
import co.cask.cdap.api.dataset.table.Scanner;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.api.service.AbstractService;
import co.cask.cdap.api.service.http.AbstractHttpServiceHandler;
import co.cask.cdap.api.service.http.HttpServiceContext;
import co.cask.cdap.api.service.http.HttpServiceRequest;
import co.cask.cdap.api.service.http.HttpServiceResponder;
import co.cask.cdap.dq.functions.BasicAggregationFunction;
import co.cask.cdap.dq.functions.CombinableAggregationFunction;
import co.cask.cdap.dq.rowkey.AggregationsRowKey;
import co.cask.cdap.dq.rowkey.ValuesRowKey;
import com.google.common.base.Charsets;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import java.io.IOException;
import java.lang.reflect.Type;
import java.net.HttpURLConnection;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;

/**
 * Service for querying values in the data quality histogram
 */
public class DataQualityService extends AbstractService {
  public static final String SERVICE_NAME = "DataQualityService";
  private static final Gson GSON = new Gson();
  private static final Type TOKEN_TYPE_SET_AGGREGATION_TYPE_VALUES =
    new TypeToken<HashSet<AggregationTypeValue>>() { }.getType();

  private final String datasetName;

  public DataQualityService(String datasetName) {
    this.datasetName = datasetName;
  }

  @Override
  protected void configure() {
    setName(SERVICE_NAME);
    setDescription("Service to query data quality histogram.");
    addHandler(new ValuesLookup(datasetName));
  }

  /**
   * Handler class for Data Quality Combinable Aggregations Service
   */
  @Path("/v1")
  public static final class ValuesLookup extends AbstractHttpServiceHandler {
    @Property
    private final String datasetName;

    Table dataStore;

    public ValuesLookup(String datasetName) {
      this.datasetName = datasetName;
    }

    @Override
    protected void configure() {
      useDatasets(datasetName);
    }

    @Override
    public void initialize(HttpServiceContext context) throws Exception {
      super.initialize(context);
      dataStore = context.getDataset(datasetName);
    }

    /**
     * Gets the fields that are queryable for a given time range and sourceID
     * for combinable aggregation functions
     */
    @Path("sources/{sourceID}/fields")
    @GET
    public void fieldsGetter(HttpServiceRequest request, HttpServiceResponder responder,
                             @PathParam("sourceID") String sourceID,
                             @QueryParam("startTimestamp") @DefaultValue("0") long startTimestamp,
                             @QueryParam("endTimestamp") @DefaultValue("9223372036854775807")
                             long endTimestamp) throws IOException {
      AggregationsRowKey aggregationsRowKeyStart = new AggregationsRowKey(startTimestamp, sourceID);
      // scan rows inclusive of endTimestamp
      AggregationsRowKey aggregationsRowKeyEnd = new AggregationsRowKey(endTimestamp + 1, sourceID);
      Scanner scanner = dataStore.scan(aggregationsRowKeyStart.getTableRowKey(),
                                       aggregationsRowKeyEnd.getTableRowKey());
      Row row;
      Map<String, FieldDetail> fieldDetailMap = new HashMap<>();
      try {
        while ((row = scanner.next()) != null) {
          Map<byte[], byte[]> columnsMapBytes = row.getColumns();
          List<FieldDetail> timestampSpecificFieldDetailList = new ArrayList<>();
          for (Map.Entry<byte[], byte[]> columnMapEntry : columnsMapBytes.entrySet()) {
            String fieldName = Bytes.toString(columnMapEntry.getKey());
            byte[] output = columnMapEntry.getValue();
            String outputString = Bytes.toString(output);
            Set<AggregationTypeValue> aggregationTypeValuesSet =
              GSON.fromJson(outputString, TOKEN_TYPE_SET_AGGREGATION_TYPE_VALUES);
            FieldDetail fieldDetail = new FieldDetail(fieldName, aggregationTypeValuesSet);
            timestampSpecificFieldDetailList.add(fieldDetail);
          }
          for (FieldDetail fdTimestampSpecific : timestampSpecificFieldDetailList) {
            String fdTimestampSpecificFieldName = fdTimestampSpecific.getFieldName();
            if (fieldDetailMap.containsKey(fdTimestampSpecificFieldName)) {
              FieldDetail fdCombined = fieldDetailMap.get(fdTimestampSpecificFieldName);
              fdCombined.addAggregations(fdTimestampSpecific.getAggregationTypeSet());
            } else {
              fieldDetailMap.put(fdTimestampSpecificFieldName, fdTimestampSpecific);
            }
          }
        }
      } finally {
        scanner.close();
      }
      if (fieldDetailMap.isEmpty()) {
        responder.sendString(HttpURLConnection.HTTP_NOT_FOUND, "No fields for the given parameters", Charsets.UTF_8);
      } else {
        responder.sendJson(HttpURLConnection.HTTP_OK, fieldDetailMap.values());
      }
    }

    /**
     * Gets the aggregation functions that are queryable for a given time range, sourceID, and field name
     */
    @Path("sources/{sourceID}/fields/{fieldName}")
    @GET
    public void aggregationTypesGetter(HttpServiceRequest request, HttpServiceResponder responder,
                                       @PathParam("fieldName") String fieldName,
                                       @PathParam("sourceID") String sourceID,
                                       @QueryParam("startTimestamp") @DefaultValue("0") long startTimestamp,
                                       @QueryParam("endTimestamp") @DefaultValue("9223372036854775807")
                                       long endTimestamp) throws IOException {
      AggregationsRowKey aggregationsRowKeyStart = new AggregationsRowKey(startTimestamp, sourceID);
      // scan rows inclusive of endTimestamp
      AggregationsRowKey aggregationsRowKeyEnd = new AggregationsRowKey(endTimestamp + 1, sourceID);
      Scanner scanner = dataStore.scan(aggregationsRowKeyStart.getTableRowKey(),
                                       aggregationsRowKeyEnd.getTableRowKey());
      Row row;
      byte[] fieldNameBytes = Bytes.toBytes(fieldName);
      Set<AggregationTypeValue> commonAggregationTypeValues = new HashSet<>();
      try {
        while ((row = scanner.next()) != null) {
          Map<byte[], byte[]> columnsMapBytes = row.getColumns();
          byte[] output = columnsMapBytes.get(fieldNameBytes);
          String outputString = Bytes.toString(output);
          Set<AggregationTypeValue> aggregationTypeValuesSet =
            GSON.fromJson(outputString, TOKEN_TYPE_SET_AGGREGATION_TYPE_VALUES);
          commonAggregationTypeValues.addAll(aggregationTypeValuesSet);
        }
      } finally {
        scanner.close();
      }
      if (commonAggregationTypeValues.isEmpty()) {
        responder.sendString(HttpURLConnection.HTTP_NOT_FOUND,
                             "No aggregation functions for the given parameters", Charsets.UTF_8);
      } else {
        responder.sendJson(HttpURLConnection.HTTP_OK, commonAggregationTypeValues);
      }
    }

    /**
     * Gets the corresponding aggregation for a given aggregation type, field name, sourceID, and time interval
     */
    @Path("sources/{sourceID}/fields/{fieldName}/aggregations/{aggregationType}/totals")
    @GET
    public void combinableAggregationGetter(HttpServiceRequest request, HttpServiceResponder responder,
                                            @PathParam("fieldName") String fieldName,
                                            @PathParam("aggregationType") String aggregationType,
                                            @PathParam("sourceID") String sourceID,
                                            @QueryParam("startTimestamp") @DefaultValue("0") long startTimestamp,
                                            @QueryParam("endTimestamp") @DefaultValue("9223372036854775807")
                                            long endTimestamp) throws IOException {
      ValuesRowKey valuesRowKeyStart = new ValuesRowKey(startTimestamp, fieldName, sourceID);
      ValuesRowKey valuesRowKeyEnd = new ValuesRowKey(endTimestamp + 1,
                                                      fieldName, sourceID); // scan rows inclusive of endTimestamp
      try {
        Class<?> aggregationClass = Class.forName("co.cask.cdap.dq.functions." + aggregationType);
        CombinableAggregationFunction aggregationClassInstance =
          (CombinableAggregationFunction) aggregationClass.newInstance();
        Scanner scanner = dataStore.scan(valuesRowKeyStart.getTableRowKey(),
                                         valuesRowKeyEnd.getTableRowKey());
        Row row;
        byte[] aggregationTypeBytes = Bytes.toBytes(aggregationType);
        try {
          while ((row = scanner.next()) != null) {
            Map<byte[], byte[]> columnsMapBytes = row.getColumns();
            byte[] output = columnsMapBytes.get(aggregationTypeBytes);
            if (output != null) {
              aggregationClassInstance.combine(output);
            }
          }
        } finally {
          scanner.close();
        }
        Object output = aggregationClassInstance.retrieveAggregation();
        if (output == null) {
          responder.sendString(HttpURLConnection.HTTP_NOT_FOUND,
                               "No aggregation for the given parameters", Charsets.UTF_8);
        } else {
          responder.sendJson(HttpURLConnection.HTTP_OK, output);
        }
      } catch (ClassNotFoundException | InstantiationException | IllegalAccessException e) {
        responder.sendString(HttpURLConnection.HTTP_NOT_FOUND,
                             String.format("Aggregation %s could not be found." , aggregationType), Charsets.UTF_8);
      } catch (ClassCastException e) {
        responder.sendString(HttpURLConnection.HTTP_BAD_REQUEST,
                             "Aggregation function is not a Combinable Aggregation Function", Charsets.UTF_8);
      }
    }

    /**
     * Gets the corresponding aggregation for a given aggregation type, field name, sourceID, and time interval
     */
    @Path("sources/{sourceID}/fields/{fieldName}/aggregations/{aggregationType}/timeseries")
    @GET
    public void basicAggregationGetter(HttpServiceRequest request, HttpServiceResponder responder,
                                       @PathParam("fieldName") String fieldName,
                                       @PathParam("aggregationType") String aggregationType,
                                       @PathParam("sourceID") String sourceID,
                                       @QueryParam("startTimestamp") @DefaultValue("0") long startTimestamp,
                                       @QueryParam("endTimestamp") @DefaultValue("9223372036854775807")
                                       long endTimestamp) throws IOException {
      ValuesRowKey valuesRowKeyStart = new ValuesRowKey(startTimestamp, fieldName, sourceID);
      ValuesRowKey valuesRowKeyEnd = new ValuesRowKey(endTimestamp + 1,
                                                      fieldName, sourceID); // scan rows inclusive of endTimestamp
      List<TimestampValue> timestampValueList = new ArrayList<>();
      try {
        Class<?> aggregationClass = Class.forName("co.cask.cdap.dq.functions." + aggregationType);
        BasicAggregationFunction aggregationClassInstance =
          (BasicAggregationFunction) aggregationClass.newInstance();
        Scanner scanner = dataStore.scan(valuesRowKeyStart.getTableRowKey(),
                                         valuesRowKeyEnd.getTableRowKey());
        Row row;
        byte[] aggregationTypeBytes = Bytes.toBytes(aggregationType);
        try {
          while ((row = scanner.next()) != null) {
            byte[] rowBytes = row.getRow();
            Long timestamp = Bytes.toLong(rowBytes, rowBytes.length - Bytes.SIZEOF_LONG);
            Map<byte[], byte[]> columnsMapBytes = row.getColumns();
            byte[] output = columnsMapBytes.get(aggregationTypeBytes);
            if (output != null) {
              Object deserializedOutput = aggregationClassInstance.deserialize(output);
              TimestampValue tsValue = new TimestampValue(timestamp, deserializedOutput);
              timestampValueList.add(tsValue);
            }
          }
        } finally {
          scanner.close();
        }
        if (timestampValueList.isEmpty()) {
          responder.sendString(HttpURLConnection.HTTP_NOT_FOUND,
                               "No aggregation for the given parameters", Charsets.UTF_8);
        } else {
          responder.sendJson(HttpURLConnection.HTTP_OK, timestampValueList);
        }
      } catch (ClassNotFoundException | InstantiationException | IllegalAccessException e) {
        responder.sendString(HttpURLConnection.HTTP_NOT_FOUND,
                             String.format("Aggregation %s could not be found.", aggregationType), Charsets.UTF_8);
      } catch (ClassCastException e) {
        responder.sendString(HttpURLConnection.HTTP_BAD_REQUEST,
                             "Aggregation function is not a Basic Aggregation Function", Charsets.UTF_8);
      }
    }
  }
}
