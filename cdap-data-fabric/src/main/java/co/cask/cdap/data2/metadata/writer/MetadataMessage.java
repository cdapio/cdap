/*
 * Copyright © 2018 Cask Data, Inc.
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

package co.cask.cdap.data2.metadata.writer;

import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.proto.id.ProgramId;
import co.cask.cdap.proto.id.ProgramRunId;
import com.google.gson.Gson;
import com.google.gson.JsonElement;

import javax.annotation.Nullable;

/**
 * A container for messages in the metadata topic configured by {@link Constants.Metadata#MESSAGING_TOPIC}.
 * It carries the message type and the payload as {@link JsonElement}.
 */
public final class MetadataMessage {

  /**
   * The message type.
   */
  public enum Type {
    LINEAGE,
    USAGE,
    WORKFLOW_TOKEN,
    WORKFLOW_STATE
  }

  private final Type type;
  private final ProgramId programId;
  @Nullable
  private final String runId;
  private final JsonElement payload;

  /**
   * Creates an instance for a program.
   *
   * @param type type of the message
   * @param programId the {@link ProgramId} of the program emitting this message
   * @param payload the payload
   */
  public MetadataMessage(Type type, ProgramId programId, JsonElement payload) {
    this(type, programId, null, payload);
  }

  /**
   * Creates an instance for a program run
   *
   * @param type type of the message
   * @param programRunId the {@link ProgramRunId} of the program run that is emitting this message
   * @param payload the payload
   */
  public MetadataMessage(Type type, ProgramRunId programRunId, JsonElement payload) {
    this(type, programRunId.getParent(), programRunId.getRun(), payload);
  }

  private MetadataMessage(Type type, ProgramId programId, @Nullable String runId, JsonElement payload) {
    this.type = type;
    this.programId = programId;
    this.runId = runId;
    this.payload = payload;
  }

  /**
   * Returns the type of the message.
   */
  public Type getType() {
    return type;
  }

  /**
   * Returns the {@link ProgramId} of the program who emit this message.
   */
  public ProgramId getProgramId() {
    return programId;
  }

  /**
   * Returns the runId of the program run who emit this message or {@code null} if the run information
   * is not available.
   */
  @Nullable
  public String getRunId() {
    return runId;
  }

  /**
   * Returns the payload by decoding the json to the given type.
   *
   * @param gson the {@link Gson} for decoding the json element
   * @param objType the resulting object type
   * @param <T> the resulting object type
   * @return the decode object
   */
  public <T> T getPayload(Gson gson, java.lang.reflect.Type objType) {
    return gson.fromJson(payload, objType);
  }

  /**
   * Returns the payload as the raw {@link JsonElement}.
   */
  public JsonElement getRawPayload() {
    return payload;
  }

  @Override
  public String toString() {
    return "MetadataMessage{" +
      "type=" + type +
      ", programId=" + programId +
      ", runId='" + runId + '\'' +
      ", payload=" + payload +
      '}';
  }
}
