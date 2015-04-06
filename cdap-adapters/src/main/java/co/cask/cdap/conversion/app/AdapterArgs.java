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

package co.cask.cdap.conversion.app;

import co.cask.cdap.api.Resources;
import co.cask.cdap.api.data.format.FormatSpecification;
import co.cask.cdap.api.data.schema.Schema;
import com.google.gson.JsonObject;

import java.io.IOException;
import java.util.Collections;

/**
 */
public class AdapterArgs {
  private final String format;
  private final JsonObject schema;
  private final String headers;
  private final String source;
  private final String sink;
  private final String frequency;

  public AdapterArgs(String format, JsonObject schema, String headers, String source, String sink, String frequency) {
    this.format = format;
    this.schema = schema;
    this.headers = headers;
    this.source = source;
    this.sink = sink;
    this.frequency = frequency;
  }

  public ConversionConfig getConfig() throws IOException {
    return new ConversionConfig(
      new Resources(),
      new FormatSpecification(format, Schema.parseJson(schema.toString()), Collections.<String, String>emptyMap()),
      source, sink, headers, frequency);
  }
}
