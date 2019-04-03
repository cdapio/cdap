/*
 * Copyright © 2019 Cask Data, Inc.
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

package co.cask.cdap.metrics.process;

import co.cask.cdap.api.metrics.MetricDeleteQuery;
import com.google.gson.Gson;
import com.google.gson.JsonElement;

/**
 * Message envelope for metrics administrative messages on TMS.
 */
public final class MetricsAdminMessage {

  /**
   * The message type.
   */
  public enum Type {
    /**
     * Indicate deletion of metrics. The payload would be Json serialization of {@link MetricDeleteQuery}.
     */
    DELETE;
  }

  private final Type type;
  private final JsonElement payload;

  /**
   * Returns the type of the message.
   */
  public Type getType() {
    return type;
  }

  /**
   * Constructs a message with the given type and payload.
   */
  public MetricsAdminMessage(Type type, JsonElement payload) {
    this.type = type;
    this.payload = payload;
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

  @Override
  public String toString() {
    return "MetricsAdminMessage{" +
      "type=" + type +
      ", payload=" + payload +
      '}';
  }
}
