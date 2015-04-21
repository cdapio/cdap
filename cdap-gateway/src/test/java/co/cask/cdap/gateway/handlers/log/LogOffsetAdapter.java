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

package co.cask.cdap.gateway.handlers.log;

import co.cask.cdap.logging.gateway.handlers.FormattedLogEvent;
import co.cask.cdap.logging.read.LogOffset;
import com.google.gson.TypeAdapter;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonWriter;

import java.io.IOException;

/**
*
*/
public final class LogOffsetAdapter extends TypeAdapter<LogOffset> {
  @Override
  public void write(JsonWriter out, LogOffset value) throws IOException {
    out.value(FormattedLogEvent.formatLogOffset(value));
  }

  @Override
  public LogOffset read(JsonReader in) throws IOException {
    return FormattedLogEvent.parseLogOffset(in.nextString());
  }
}
