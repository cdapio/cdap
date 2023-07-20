/*
 * Copyright © 2023 Cask Data, Inc.
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

package io.cdap.cdap.internal.credential.handler;

import com.google.gson.TypeAdapter;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonWriter;
import java.io.IOException;
import java.time.Instant;

/**
 * Type adapter which converts an {@link Instant} to a timestamp in seconds.
 */
public class InstantEpochSecondsTypeAdapter extends TypeAdapter<Instant> {

  @Override
  public void write(JsonWriter jsonWriter, Instant instant) throws IOException {
    jsonWriter.value(instant.getEpochSecond());
  }

  @Override
  public Instant read(JsonReader jsonReader) throws IOException {
    return Instant.ofEpochSecond(jsonReader.nextLong());
  }
}
