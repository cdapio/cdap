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
package co.cask.cdap.data.format;

import co.cask.cdap.api.data.schema.Schema;
import oi.thekraken.grok.api.Grok;

import java.util.Map;

/**
 * GrokRecordFormat
 */
public class SyslogRecordFormat extends GrokRecordFormat {

  @Override
  protected Schema getDefaultSchema() {
    return Schema.recordOf(
      "streamEvent",
      Schema.Field.of("timestamp", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
      Schema.Field.of("logsource", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
      Schema.Field.of("program", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
      Schema.Field.of("message", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
      Schema.Field.of("pid", Schema.nullableOf(Schema.of(Schema.Type.STRING))));
  }

  @Override
  protected void addPatterns(Grok grok) {
    addPattern(grok, "cdap/grok/patterns/grok-patterns");
    addPattern(grok, "cdap/grok/patterns/linux-syslog");
  }

  @Override
  protected String determinePattern(Map<String, String> settings) {
    return "%{SYSLOGLINE:syslogline}";
  }
}
