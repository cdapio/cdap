/*
 * Copyright © 2016 Cask Data, Inc.
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

package co.cask.cdap.gateway.handlers;

import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.common.BadRequestException;
import co.cask.cdap.common.app.RunIds;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.internal.io.SchemaTypeAdapter;
import co.cask.cdap.logging.gateway.handlers.FormattedLogEvent;
import co.cask.cdap.logging.read.LogOffset;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.element.EntityType;
import co.cask.cdap.proto.id.EntityId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.NamespacedId;
import co.cask.cdap.proto.id.ParentedId;
import co.cask.http.AbstractHttpHandler;
import co.cask.http.HttpResponder;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nullable;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;

/**
 * {@link co.cask.http.HttpHandler} to manage program preview lifecycle.
 */
@Singleton
@Path(Constants.Gateway.API_VERSION_3 + "/namespaces/{namespace-id}")
public class PreviewHttpHandler extends AbstractHttpHandler {
  private static final Logger LOG = LoggerFactory.getLogger(PreviewHttpHandler.class);
  private static final Gson GSON = new GsonBuilder()
    .registerTypeAdapter(Schema.class, new SchemaTypeAdapter())
    .create();


  @Inject
  PreviewHttpHandler() {
  }

  /**
   *
   * @param request
   * @param responder
   * @param namespaceId
   * @throws IOException
   * @throws BadRequestException
   */
  @POST
  @Path("/preview")
  public void startPreview(HttpRequest request, HttpResponder responder,
                           @PathParam("namespace-id") String namespaceId) throws IOException, BadRequestException {
    String previewId = RunIds.generate().getId();
    responder.sendString(HttpResponseStatus.OK, GSON.toJson(new PreviewId(namespaceId, previewId)));
  }

  /**
   *
   * @param request
   * @param responder
   * @param namespaceId
   * @param emitterId
   */
  @GET
  @Path("/previews/{preview-id}/emitters/{emitter-id}")
  public void getPreviewData(HttpRequest request, HttpResponder responder,
                             @PathParam("namespace-id") String namespaceId,
                             @PathParam("emitter-id") String emitterId) {

  }

  /**
   *
   */
  public static class PreviewStatus {
    /**
     *
     */
    public enum Status {
      RUNNING,
      COMPLETED,
      DEPLOY_FAILED,
      RUN_FAILED
    }

    Status status;

    @Nullable
    String failureMessage;

    public PreviewStatus(Status status, @Nullable String failureMessage) {
      this.status = status;
      this.failureMessage = failureMessage;
    }

    public Status getStatus() {
      return status;
    }

    public String getFailureMessage() {
      return failureMessage;
    }
  }

  /**
   *
   */
  public class PreviewId extends EntityId implements NamespacedId, ParentedId<NamespaceId> {

    private final String namespace;
    private final String preview;

    public PreviewId(String namespace, String preview) {
      super(EntityType.APPLICATION);
      this.namespace = namespace;
      this.preview = preview;
    }

    @Override
    protected Iterable<String> toIdParts() {
      return null;
    }

    @Override
    public Id toId() {
      return null;
    }

    @Override
    public String getNamespace() {
      return namespace;
    }

    @Override
    public NamespaceId getParent() {
      return new NamespaceId(namespace);
    }
  }

  /**
   *
   * @param request
   * @param responder
   * @param namespaceId
   * @param previewId
   */
  @GET
  @Path("/previews/{preview-id}/status")
  public void getPreviewStatus(HttpRequest request, HttpResponder responder,
                               @PathParam("namespace-id") String namespaceId,
                               @PathParam("preview-id") String previewId) {
    PreviewStatus status = new PreviewStatus(PreviewStatus.Status.RUN_FAILED,
                                             "Preview Failed. Reason: No FileSystem for scheme: maprfs.");
    responder.sendString(HttpResponseStatus.OK, GSON.toJson(status));
  }

  /**
   *
   * @param request
   * @param responder
   * @param namespaceId
   * @param previewId
   * @param stageId
   */
  @GET
  @Path("/previews/{preview-id}/stages/{stage-id}")
  public void getPreviewStageData(HttpRequest request, HttpResponder responder,
                                  @PathParam("namespace-id") String namespaceId,
                                  @PathParam("preview-id") String previewId, @PathParam("stage-id") String stageId) {
    responder.sendString(HttpResponseStatus.OK, GSON.toJson(new PipelinePreviewData()));
  }

  @GET
  @Path("/previews/{preview-id}/logs")
  public void getLogs(HttpRequest request, HttpResponder responder,
                      @PathParam("namespace-id") String namespaceId) {
    List<FormattedLogEvent> logResults = new ArrayList<>();
    for (int i = 0; i < 100; i++) {
      FormattedLogEvent event
        = new FormattedLogEvent("This is sample log - " + i, new LogOffset(i, System.currentTimeMillis()));
      logResults.add(event);
    }
    responder.sendString(HttpResponseStatus.OK, GSON.toJson(logResults));
  }

  @GET
  @Path("/previews/{preview-id}/metrics")
  public void getMetric(HttpRequest request, HttpResponder responder,
                        @PathParam("namespace-id") String namespaceId) {

    String response = "{\n" +
      "    \"endTime\": 1466469538,\n" +
      "    \"resolution\": \"2147483647s\",\n" +
      "    \"series\": [\n" +
      "        {\n" +
      "            \"data\": [\n" +
      "                {\n" +
      "                    \"time\": 0,\n" +
      "                    \"value\": 4\n" +
      "                }\n" +
      "            ],\n" +
      "            \"grouping\": {},\n" +
      "            \"metricName\": \"user.Projection.records.out\"\n" +
      "        },\n" +
      "        {\n" +
      "            \"data\": [\n" +
      "                {\n" +
      "                    \"time\": 0,\n" +
      "                    \"value\": 4\n" +
      "                }\n" +
      "            ],\n" +
      "            \"grouping\": {},\n" +
      "            \"metricName\": \"user.Projection.records.in\"\n" +
      "        },\n" +
      "        {\n" +
      "            \"data\": [\n" +
      "                {\n" +
      "                    \"time\": 0,\n" +
      "                    \"value\": 4\n" +
      "                }\n" +
      "            ],\n" +
      "            \"grouping\": {},\n" +
      "            \"metricName\": \"user.Stream.records.out\"\n" +
      "        },\n" +
      "        {\n" +
      "            \"data\": [\n" +
      "                {\n" +
      "                    \"time\": 0,\n" +
      "                    \"value\": 4\n" +
      "                }\n" +
      "            ],\n" +
      "            \"grouping\": {},\n" +
      "            \"metricName\": \"user.JavaScript.records.in\"\n" +
      "        },\n" +
      "        {\n" +
      "            \"data\": [\n" +
      "                {\n" +
      "                    \"time\": 0,\n" +
      "                    \"value\": 4\n" +
      "                }\n" +
      "            ],\n" +
      "            \"grouping\": {},\n" +
      "            \"metricName\": \"user.Stream.records.in\"\n" +
      "        }\n" +
      "    ],\n" +
      "    \"startTime\": 0\n" +
      "}\n";

    responder.sendString(HttpResponseStatus.OK, response);
  }

  /**
   *
   */
  public static class InputRecord {
    public Long offset;
    public String body;

    public InputRecord(Long offset, String body) {
      this.offset = offset;
      this.body = body;
    }
  }

  /**
   *
   */
  public static class OutputRecord {
    public String firstName;
    public String lastName;
    public Long zipCode;

    public OutputRecord(String firstName, String lastName, Long zipCode) {
      this.firstName = firstName;
      this.lastName = lastName;
      this.zipCode = zipCode;
    }
  }

  /**
   *
   */
  public static class ErrorRecord {
    public int errCode;
    public String errMsg;
    public String invalidRecord;

    public ErrorRecord(int errCode, String errMsg, String invalidRecord) {
      this.errCode = errCode;
      this.errMsg = errMsg;
      this.invalidRecord = invalidRecord;
    }
  }

  /**
   *
   */
  public static class PipelinePreviewData {

    List<InputRecord> inputData = new ArrayList<>();
    List<OutputRecord> outputData = new ArrayList<>();
    List<ErrorRecord> errorRecords = new ArrayList<>();
    Schema inputSchema;
    Schema outputSchema;
    Schema errorRecordSchema;

    public PipelinePreviewData() {
      inputSchema = Schema.recordOf("inputRecordSchema", Schema.Field.of("offset", Schema.of(Schema.Type.LONG)),
                                    Schema.Field.of("body", Schema.of(Schema.Type.STRING)));
      outputSchema = Schema.recordOf("outputRecordSchema", Schema.Field.of("firstName", Schema.of(Schema.Type.STRING)),
                                     Schema.Field.of("lastName", Schema.of(Schema.Type.STRING)),
                                     Schema.Field.of("zipCode", Schema.of(Schema.Type.LONG)));
      errorRecordSchema = Schema.recordOf("errorRecordSchema", Schema.Field.of("errCode", Schema.of(Schema.Type.INT)),
                                     Schema.Field.of("errMsg", Schema.of(Schema.Type.STRING)),
                                     Schema.Field.of("invalidRecord", Schema.of(Schema.Type.STRING)));
      inputData.add(new InputRecord(1L, "jim,carry,95054"));
      inputData.add(new InputRecord(2L, "jerry,seinfeld,95055"));
      inputData.add(new InputRecord(3L, "This error record is not comma separated!"));
      inputData.add(new InputRecord(4L, "jimmy,kimmel,95056"));
      inputData.add(new InputRecord(5L, "bill,cosby,95057"));
      inputData.add(new InputRecord(6L, "david,letterman,95058"));

      outputData.add(new OutputRecord("jim", "carry", 95054L));
      outputData.add(new OutputRecord("jerry", "seinfeld", 95055L));
      outputData.add(new OutputRecord("jimmy", "kimmel", 95056L));
      outputData.add(new OutputRecord("bill", "cosby", 95057L));
      outputData.add(new OutputRecord("david", "letterman", 95058L));

      errorRecords.add(new ErrorRecord(12, "Invalid record",
                                       GSON.toJson(new InputRecord(3L, "This error record is not comma separated!"))));

    }
  }
}
