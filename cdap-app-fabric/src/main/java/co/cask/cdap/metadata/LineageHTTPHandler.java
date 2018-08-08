/*
 * Copyright © 2015-2018 Cask Data, Inc.
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

package co.cask.cdap.metadata;

import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Service;
import co.cask.cdap.api.lineage.field.EndPoint;
import co.cask.cdap.common.BadRequestException;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.utils.TimeMathParser;
import co.cask.cdap.data2.metadata.lineage.Lineage;
import co.cask.cdap.data2.metadata.lineage.LineageSerializer;
import co.cask.cdap.data2.metadata.lineage.field.EndPointField;
import co.cask.cdap.proto.codec.NamespacedEntityIdCodec;
import co.cask.cdap.proto.id.DatasetId;
import co.cask.cdap.proto.id.NamespacedEntityId;
import co.cask.cdap.proto.id.StreamId;
import co.cask.cdap.proto.metadata.lineage.CollapseType;
import co.cask.cdap.proto.metadata.lineage.FieldLineageDetails;
import co.cask.cdap.proto.metadata.lineage.FieldLineageSummary;
import co.cask.cdap.proto.metadata.lineage.LineageRecord;
import co.cask.http.AbstractHttpHandler;
import co.cask.http.HttpResponder;
import com.google.common.base.Joiner;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.inject.Inject;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
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
@Service(Constants.Service.METADATA_SERVICE)
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

  @GET
  @Path("/namespaces/{namespace-id}/datasets/{dataset-id}/lineage/fields")
  public void datasetFields(HttpRequest request, HttpResponder responder,
                            @PathParam("namespace-id") String namespaceId,
                            @PathParam("dataset-id") String datasetId,
                            @QueryParam("start") String startStr,
                            @QueryParam("end") String endStr,
                            @QueryParam("prefix") String prefix) throws BadRequestException {
    TimeRange range = parseRange(startStr, endStr);
    Set<String> result = fieldLineageAdmin.getFields(EndPoint.of(namespaceId, datasetId), range.getStart(),
                                                     range.getEnd(), prefix);
    responder.sendJson(HttpResponseStatus.OK, GSON.toJson(result));
  }

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
    FieldLineageSummary summary = fieldLineageAdmin.getSummary(direction, endPointField, range.getStart(),
                                                               range.getEnd());
    responder.sendJson(HttpResponseStatus.OK, GSON.toJson(summary));
  }

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

  @GET
  @Path("/namespaces/{namespace-id}/streams/{stream-id}/lineage")
  public void streamLineage(HttpRequest request, HttpResponder responder,
                            @PathParam("namespace-id") String namespaceId,
                            @PathParam("stream-id") String stream,
                            @QueryParam("start") String startStr,
                            @QueryParam("end") String endStr,
                            @QueryParam("levels") @DefaultValue("10") int levels,
                            @QueryParam("collapse") List<String> collapse,
                            @QueryParam("rollup") String rollup) throws Exception {

    checkLevels(levels);
    TimeRange range = parseRange(startStr, endStr);

    StreamId streamId = new StreamId(namespaceId, stream);
    Lineage lineage = lineageAdmin.computeLineage(streamId, range.getStart(), range.getEnd(), levels, rollup);
    responder.sendJson(HttpResponseStatus.OK,
                       GSON.toJson(LineageSerializer.toLineageRecord(
                         TimeUnit.MILLISECONDS.toSeconds(range.getStart()),
                         TimeUnit.MILLISECONDS.toSeconds(range.getEnd()),
                         lineage, getCollapseTypes(collapse)), LineageRecord.class));
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

  private Constants.FieldLineage.Direction parseDirection(String directionStr) throws BadRequestException {
    try {
      return Constants.FieldLineage.Direction.valueOf(directionStr.toUpperCase());
    } catch (IllegalArgumentException e) {
      String directionValues = Joiner.on(", ").join(Constants.FieldLineage.Direction.values());
      throw new BadRequestException(String.format("Direction must be specified to get the field lineage " +
                                                    "summary and should be one of the following: [%s].",
                                                  directionValues.toLowerCase()));
    }
  }
}
