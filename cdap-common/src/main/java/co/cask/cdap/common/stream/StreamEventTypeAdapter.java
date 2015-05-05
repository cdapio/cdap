/*
 * Copyright © 2014 Cask Data, Inc.
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

package co.cask.cdap.common.stream;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.flow.flowlet.StreamEvent;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.TypeAdapter;
import com.google.gson.TypeAdapterFactory;
import com.google.gson.reflect.TypeToken;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
import com.google.gson.stream.JsonWriter;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;

/**
 * A GSon {@link TypeAdapter} for serializing/deserializing {@link StreamEvent} to/from JSON. It serializes
 *
 * StreamEvent into
 * <p>
 * {@code {"timestamp": ... , "headers": { ... }, "body": ... }}
 * </p><p>
 * where the body is encoded as a string binary using Bytes.toStringBinary.
 * </p>
 */
public class StreamEventTypeAdapter extends TypeAdapter<StreamEvent> {

  private static final TypeToken<Map<String, String>> HEADERS_TYPE = new TypeToken<Map<String, String>>() { };

  private final TypeAdapter<Map<String, String>> mapTypeAdapter;

  /**
   * Register an instance of the {@link StreamEventTypeAdapter} to the given {@link GsonBuilder}.
   * @param gsonBuilder The build to register to
   * @return The same {@link GsonBuilder} instance in the argument
   */
  public static GsonBuilder register(GsonBuilder gsonBuilder) {
    return gsonBuilder.registerTypeAdapterFactory(new TypeAdapterFactory() {
      @Override
      @SuppressWarnings("unchecked")
      public <T> TypeAdapter<T> create(Gson gson, TypeToken<T> type) {
        if (StreamEvent.class.isAssignableFrom(type.getRawType())) {
          return (TypeAdapter<T>) new StreamEventTypeAdapter(gson.getAdapter(HEADERS_TYPE));
        }
        return null;
      }
    });
  }

  private StreamEventTypeAdapter(TypeAdapter<Map<String, String>> mapTypeAdapter) {
    this.mapTypeAdapter = mapTypeAdapter;
  }

  @Override
  public void write(JsonWriter out, StreamEvent event) throws IOException {
    out.beginObject()
       .name("timestamp").value(event.getTimestamp())
       .name("headers");
    mapTypeAdapter.write(out, event.getHeaders());

    out.name("body");
    ByteBuffer body = event.getBody();
    if (body.hasArray()) {
      // Need to use ByteBuffer.slice() to make sure position starts at 0, which the toStringBinary requires.
      out.value(Bytes.toStringBinary(body.slice()));
    } else {
      // Slow path, if the byte buffer doesn't have array
      byte[] bytes = new byte[body.remaining()];
      body.mark();
      body.get(bytes);
      body.reset();
      out.value(Bytes.toStringBinary(bytes));
    }
    out.endObject();
  }

  @Override
  public StreamEvent read(JsonReader in) throws IOException {
    long timestamp = -1;
    Map<String, String> headers = null;
    ByteBuffer body = null;

    in.beginObject();
    while (in.peek() == JsonToken.NAME) {
      String key = in.nextName();
      if ("timestamp".equals(key)) {
        timestamp = in.nextLong();
      } else if ("headers".equals(key)) {
        headers = mapTypeAdapter.read(in);
      } else if ("body".equals(key)) {
        body = ByteBuffer.wrap(Bytes.toBytesBinary(in.nextString()));
      } else {
        in.skipValue();
      }
    }

    if (timestamp >= 0 && headers != null && body != null) {
      in.endObject();
      return new StreamEvent(headers, body, timestamp);
    }
    throw new IOException(String.format("Failed to read StreamEvent. Timestamp: %d, headers: %s, body: %s",
                                        timestamp, headers, body));
  }
}
