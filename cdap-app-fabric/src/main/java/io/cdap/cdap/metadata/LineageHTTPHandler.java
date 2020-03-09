/*
 * Copyright © 2015-2019 Cask Data, Inc.
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

package io.cdap.cdap.metadata;

import com.google.common.base.Joiner;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.inject.Inject;
import io.cdap.cdap.api.lineage.field.EndPoint;
import io.cdap.cdap.common.BadRequestException;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.utils.TimeMathParser;
import io.cdap.cdap.data2.metadata.lineage.Lineage;
import io.cdap.cdap.data2.metadata.lineage.LineageSerializer;
import io.cdap.cdap.data2.metadata.lineage.field.EndPointField;
import io.cdap.cdap.proto.codec.NamespacedEntityIdCodec;
import io.cdap.cdap.proto.id.DatasetId;
import io.cdap.cdap.proto.id.NamespacedEntityId;
import io.cdap.cdap.proto.metadata.lineage.CollapseType;
import io.cdap.cdap.proto.metadata.lineage.Field;
import io.cdap.cdap.proto.metadata.lineage.FieldLineageDetails;
import io.cdap.cdap.proto.metadata.lineage.FieldLineageSummary;
import io.cdap.cdap.proto.metadata.lineage.LineageRecord;
import io.cdap.http.AbstractHttpHandler;
import io.cdap.http.HttpResponder;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;

/**
 * HttpHandler for lineage.
 */
@Path(Constants.Gateway.API_VERSION_3)
public class LineageHTTPHandler extends AbstractHttpHandler {
  private static final Gson GSON = new GsonBuilder()
    .registerTypeAdapter(NamespacedEntityId.class, new NamespacedEntityIdCodec())
    .create();

  private final LineageAdmin lineageAdmin;
  private final FieldLineageAdmin fieldLineageAdmin;

  @Inject
  LineageHTTPHandler(LineageAdmin lineageAdmin, FieldLineageAdmin fieldLineageAdmin) {
    this.lineageAdmin = lineageAdmin;
    this.fieldLineageAdmin = fieldLineageAdmin;
  }

  /**
   * Get the lineage information about a dataset. This method does not give any information on field level lineage.
   *
   * @param startStr the start time string, it can be a specific timestamp in milliseconds or a relative time,
   *                 using now and times added to it.
   * @param endStr the end time string, it can be a specific timestamp in milliseconds or a relative time,
   *               using now and times added to it.
   * @param levels the level to compute the lineage
   * @param collapse indicate how to collapse the lineage result
   * @param rollup indicates whether to aggregate programs, currently supports rolling up programs into workflows
   */
  @GET
  @Path("/namespaces/{namespace-id}/datasets/{dataset-id}/lineage")
  public void datasetLineage(HttpRequest request, HttpResponder responder,
                             @PathParam("namespace-id") String namespaceId,
                             @PathParam("dataset-id") String datasetId,
                             @QueryParam("start") String startStr,
                             @QueryParam("end") String endStr,
                             @QueryParam("levels") @DefaultValue("10") int levels,
                             @QueryParam("collapse") List<String> collapse,
                             @QueryParam("rollup") String rollup) throws Exception {

    checkLevels(levels);
    TimeRange range = parseRange(startStr, endStr);

    DatasetId datasetInstance = new DatasetId(namespaceId, datasetId);
    Lineage lineage = lineageAdmin.computeLineage(datasetInstance, range.getStart(), range.getEnd(),
                                                  levels, rollup);
    responder.sendJson(HttpResponseStatus.OK,
                       GSON.toJson(LineageSerializer.toLineageRecord(
                         TimeUnit.MILLISECONDS.toSeconds(range.getStart()),
                         TimeUnit.MILLISECONDS.toSeconds(range.getEnd()),
                         lineage, getCollapseTypes(collapse)), LineageRecord.class));
  }

  /**
   * Get the fields in the dataset. This method can be used to get fields that have lineage or all the fields in the
   * dataset.
   *
   * @param startStr the start time string, it can be a specific timestamp in milliseconds or a relative time,
   *                 using now and times added to it.
   * @param endStr the end time string, it can be a specific timestamp in milliseconds or a relative time,
   *               using now and times added to it.
   * @param prefix the prefix of the field, if not provided, all fields will get return
   * @param includeCurrent a boolean to indicate whether to include fields that do not have field level lineage
   */
  @GET
  @Path("/namespaces/{namespace-id}/datasets/{dataset-id}/lineage/fields")
  public void datasetFields(HttpRequest request, HttpResponder responder,
                            @PathParam("namespace-id") String namespaceId,
                            @PathParam("dataset-id") String datasetId,
                            @QueryParam("start") String startStr,
                            @QueryParam("end") String endStr,
                            @QueryParam("prefix") String prefix,
                            @QueryParam("includeCurrent") boolean includeCurrent)
    throws BadRequestException, IOException {
    TimeRange range = parseRange(startStr, endStr);
    Set<Field> result = fieldLineageAdmin.getFields(EndPoint.of(namespaceId, datasetId), range.getStart(),
                                                    range.getEnd(), prefix, includeCurrent);
    // CDAP-14168: From 5.1 this endpoint supports returning a Set of Field object rather Set of String. For backward
    // compatibility in 5.1 the default behavior is to return a Set of String (field names). This default behavior
    // can be overridden by passing the query parameter 'includeCurrent' set to 'true' which will return set of
    // Field object.
    if (includeCurrent) {
      responder.sendJson(HttpResponseStatus.OK, GSON.toJson(result));
    } else {
      responder.sendJson(HttpResponseStatus.OK,
                         GSON.toJson(result.stream().map(Field::getName).collect(Collectors.toSet())));
    }
  }

  /**
   * Get all the field level lineage about all fields in one dataset.
   *
   * @param directionStr the direction to compute the field level lineage, can be INCOMING, OUTGOING or BOTH
   * @param startStr the start time string, it can be a specific timestamp in milliseconds or a relative time,
   *                 using now and times added to it.
   * @param endStr the end time string, it can be a specific timestamp in milliseconds or a relative time,
   *               using now and times added to it.
   */
  @GET
  @Path("/namespaces/{namespace-id}/datasets/{dataset-id}/lineage/allfieldlineage")
  public void datasetFieldLineage(HttpRequest request, HttpResponder responder,
                                  @PathParam("namespace-id") String namespaceId,
                                  @PathParam("dataset-id") String datasetId,
                                  @QueryParam("direction") String directionStr,
                                  @QueryParam("start") String startStr,
                                  @QueryParam("end") String endStr) throws BadRequestException, IOException {
    TimeRange range = parseRange(startStr, endStr);
    Constants.FieldLineage.Direction direction = parseDirection(directionStr);
    DatasetFieldLineageSummary summary = fieldLineageAdmin.getDatasetFieldLineage(direction,
                                                                                  EndPoint.of(namespaceId, datasetId),
                                                                                  range.getStart(), range.getEnd());
    responder.sendJson(HttpResponseStatus.OK, GSON.toJson(summary));
  }

  /**
   * Get the field level lineage about the specified field in one dataset.
   *
   * @param field the field name to compute field level lineage
   * @param directionStr the direction to compute the field level lineage, can be INCOMING, OUTGOING or BOTH
   * @param startStr the start time string, it can be a specific timestamp in milliseconds or a relative time,
   *                 using now and times added to it.
   * @param endStr the end time string, it can be a specific timestamp in milliseconds or a relative time,
   *               using now and times added to it.
   */
  @GET
  @Path("/namespaces/{namespace-id}/datasets/{dataset-id}/lineage/fields/{field-name}")
  public void datasetFieldLineageSummary(HttpRequest request, HttpResponder responder,
                                         @PathParam("namespace-id") String namespaceId,
                                         @PathParam("dataset-id") String datasetId,
                                         @PathParam("field-name") String field,
                                         @QueryParam("direction") String directionStr,
                                         @QueryParam("start") String startStr,
                                         @QueryParam("end") String endStr) throws BadRequestException {
    TimeRange range = parseRange(startStr, endStr);
    Constants.FieldLineage.Direction direction = parseDirection(directionStr);
    EndPointField endPointField = new EndPointField(EndPoint.of(namespaceId, datasetId), field);
    FieldLineageSummary summary = fieldLineageAdmin.getFieldLineage(direction, endPointField, range.getStart(),
                                                                    range.getEnd());
    responder.sendJson(HttpResponseStatus.OK, GSON.toJson(summary));
  }

  /**
   * Get the operation details about the specified field in one dataset.
   *
   * @param field the field name to compute field operation details
   * @param directionStr the direction to compute the field level lineage, can be INCOMING, OUTGOING or BOTH
   * @param startStr the start time string, it can be a specific timestamp in milliseconds or a relative time,
   *                 using now and times added to it.
   * @param endStr the end time string, it can be a specific timestamp in milliseconds or a relative time,
   *               using now and times added to it.
   */
  @GET
  @Path("/namespaces/{namespace-id}/datasets/{dataset-id}/lineage/fields/{field-name}/operations")
  public void datasetFieldLineageDetails(HttpRequest request, HttpResponder responder,
                                         @PathParam("namespace-id") String namespaceId,
                                         @PathParam("dataset-id") String datasetId,
                                         @PathParam("field-name") String field,
                                         @QueryParam("direction") @DefaultValue("both") String directionStr,
                                         @QueryParam("start") String startStr,
                                         @QueryParam("end") String endStr) throws BadRequestException {
    TimeRange range = parseRange(startStr, endStr);
    Constants.FieldLineage.Direction direction = parseDirection(directionStr);
    EndPointField endPointField = new EndPointField(EndPoint.of(namespaceId, datasetId), field);
    FieldLineageDetails details = fieldLineageAdmin.getOperationDetails(direction, endPointField, range.getStart(),
                                                                        range.getEnd());
    responder.sendJson(HttpResponseStatus.OK, GSON.toJson(details));
  }

  private void checkLevels(int levels) throws BadRequestException {
    if (levels < 1) {
      throw new BadRequestException(String.format("Invalid levels (%d), should be greater than 0.", levels));
    }
  }

  private static Set<CollapseType> getCollapseTypes(@Nullable List<String> collapse) throws BadRequestException {
    if (collapse == null) {
      return Collections.emptySet();
    }

    Set<CollapseType> collapseTypes = new HashSet<>();
    for (String c : collapse) {
      try {
        CollapseType type = CollapseType.valueOf(c.toUpperCase());
        collapseTypes.add(type);
      } catch (IllegalArgumentException e) {
        throw new BadRequestException(String.format("Invalid collapse type %s", c));
      }
    }
    return collapseTypes;
  }

  // TODO: CDAP-3715 This is a fairly common operation useful in various handlers
  private TimeRange parseRange(String startStr, String endStr) throws BadRequestException {
    if (startStr == null) {
      throw new BadRequestException("Start time is required.");
    }
    if (endStr == null) {
      throw new BadRequestException("End time is required.");
    }

    long now = TimeMathParser.nowInSeconds();
    long start;
    long end;
    try {
      // start and end are specified in seconds from HTTP request,
      // but specified in milliseconds to the LineageGenerator
      start = TimeUnit.SECONDS.toMillis(TimeMathParser.parseTimeInSeconds(now, startStr));
      end = TimeUnit.SECONDS.toMillis(TimeMathParser.parseTimeInSeconds(now, endStr));
    } catch (IllegalArgumentException e) {
      throw new BadRequestException(e);
    }

    if (start < 0) {
      throw new BadRequestException(String.format("Invalid start time (%s -> %d), should be >= 0.", startStr, start));
    }
    if (end < 0) {
      throw new BadRequestException(String.format("Invalid end time (%s -> %d), should be >= 0.", endStr, end));
    }
    if (start > end) {
      throw new BadRequestException(String.format("Start time (%s -> %d) should be lesser than end time (%s -> %d).",
                                                  startStr, start, endStr, end));
    }

    return new TimeRange(start, end);
  }

  private static class TimeRange {
    private final long start;
    private final long end;

    private TimeRange(long start, long end) {
      this.start = start;
      this.end = end;
    }

    public long getStart() {
      return start;
    }

    public long getEnd() {
      return end;
    }
  }

  private Constants.FieldLineage.Direction parseDirection(@Nullable String directionStr) throws BadRequestException {
    try {
      return Constants.FieldLineage.Direction.valueOf(directionStr.toUpperCase());
    } catch (NullPointerException | IllegalArgumentException e) {
      String directionValues = Joiner.on(", ").join(Constants.FieldLineage.Direction.values());
      throw new BadRequestException(String.format("Direction must be specified to get the field lineage " +
                                                    "summary and should be one of the following: [%s].",
                                                  directionValues.toLowerCase()));
    }
  }
}
