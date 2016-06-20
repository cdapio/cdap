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
import co.cask.http.AbstractHttpHandler;
import co.cask.http.HttpResponder;
import com.google.gson.Gson;
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
  private static final Gson GSON = new Gson();

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
    responder.sendString(HttpResponseStatus.OK, previewId);
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
    PreviewStatus status = new PreviewStatus(PreviewStatus.Status.RUN_FAILED, "No FileSystem for scheme: maprfs");
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
    public String first_name;
    public String last_name;
    public Long zip_code;

    public OutputRecord(String first_name, String last_name, Long zip_code) {
      this.first_name = first_name;
      this.last_name = last_name;
      this.zip_code = zip_code;
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
      outputSchema = Schema.recordOf("outputRecordSchema", Schema.Field.of("first_name", Schema.of(Schema.Type.STRING)),
                                     Schema.Field.of("last_name", Schema.of(Schema.Type.STRING)),
                                     Schema.Field.of("zip_code", Schema.of(Schema.Type.LONG)));
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
