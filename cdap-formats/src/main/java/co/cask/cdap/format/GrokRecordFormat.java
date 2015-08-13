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
package co.cask.cdap.format;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.format.UnexpectedFormatException;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.data.schema.UnsupportedTypeException;
import co.cask.cdap.api.flow.flowlet.StreamEvent;
import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Resources;
import oi.thekraken.grok.api.Grok;
import oi.thekraken.grok.api.Match;
import oi.thekraken.grok.api.exception.GrokException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.StringReader;
import java.net.URL;
import java.util.Iterator;
import java.util.Map;

/**
 * GrokRecordFormat. Grok parses a string and outputs a map of field name (string) to value (string).
 */
public class GrokRecordFormat extends StreamEventRecordFormat<StructuredRecord> {

  private static final Logger LOG = LoggerFactory.getLogger(GrokRecordFormat.class);
  private static final String DEFAULT_PATTERN = "%{GREEDYDATA:body}";
  private static final String PATTERN_SETTING = "pattern";

  private final Grok grok = new Grok();
  private String pattern = null;

  public static Map<String, String> settings(String pattern) {
    return ImmutableMap.of(PATTERN_SETTING, pattern);
  }

  @Override
  public StructuredRecord read(StreamEvent event) throws UnexpectedFormatException {
    String bodyAsStr = Bytes.toString(event.getBody(), Charsets.UTF_8);
    StructuredRecord.Builder builder = StructuredRecord.builder(schema);

    Match gm = grok.match(bodyAsStr);
    gm.captures();
    Map<String, Object> x = gm.toMap();

    for (Schema.Field field : schema.getFields()) {
      String fieldName = field.getName();
      Object value = x.get(fieldName);
      if (value != null) {
        builder.convertAndSet(fieldName, value.toString());
      }
    }

    return builder.build();
  }

  @Override
  protected Schema getDefaultSchema() {
    // default is a String[]
    return Schema.recordOf("streamEvent", Schema.Field.of("body", Schema.nullableOf(Schema.of(Schema.Type.STRING))));
  }

  @Override
  protected void validateSchema(Schema desiredSchema) throws UnsupportedTypeException {
    // a valid schema is a record of simple types. In other words, no maps, arrays, records, unions, or enums allowed.
    // the exception is the very last field, which is allowed to be an array of simple types.
    // These types may be nullable, which is a union of a null and non-null type.
    Iterator<Schema.Field> fields = desiredSchema.getFields().iterator();
    // check that each field is a simple field, except for the very last field, which can be an array of simple types.
    while (fields.hasNext()) {
      Schema.Field field = fields.next();
      Schema schema = field.getSchema();
      // if we're not on the very last field, the field must be a simple type or a nullable simple type.
      boolean isSimple = schema.getType().isSimpleType();
      boolean isNullableSimple = schema.isNullableSimple();
      if (!isSimple && !isNullableSimple) {
        // if this is the very last field and a string array, it is valid. otherwise it is not.
        if (fields.hasNext() || !isStringArray(schema)) {
          throw new UnsupportedTypeException("Field " + field.getName() + " is of invalid type.");
        }
      }
    }
  }

  // check that it's an array of strings or array of nullable strings. the array itself can also be nullable.
  private boolean isStringArray(Schema schema) {
    Schema arrSchema = schema.isNullable() ? schema.getNonNullable() : schema;
    if (arrSchema.getType() == Schema.Type.ARRAY) {
      Schema componentSchema = arrSchema.getComponentSchema();
      if (componentSchema.isNullable()) {
        return componentSchema.getNonNullable().getType() == Schema.Type.STRING;
      } else {
        return componentSchema.getType() == Schema.Type.STRING;
      }
    }
    return false;
  }

  @Override
  protected void configure(Map<String, String> settings) {
    addPatterns(grok);

    try {
      this.pattern = determinePattern(settings);
      grok.compile(pattern);
    } catch (GrokException e) {
      LOG.error("Failed to compile grok pattern '{}'", pattern, e);
    }
  }

  protected void addPatterns(Grok grok) {
    addPattern(grok, "cdap/grok/patterns/firewalls");
    addPattern(grok, "cdap/grok/patterns/grok-patterns");
    addPattern(grok, "cdap/grok/patterns/haproxy");
    addPattern(grok, "cdap/grok/patterns/java");
    addPattern(grok, "cdap/grok/patterns/junos");
    addPattern(grok, "cdap/grok/patterns/linux-syslog");
    addPattern(grok, "cdap/grok/patterns/mcollective");
    addPattern(grok, "cdap/grok/patterns/mcollective-patterns");
    addPattern(grok, "cdap/grok/patterns/mongodb");
    addPattern(grok, "cdap/grok/patterns/nagios");
    addPattern(grok, "cdap/grok/patterns/postgresql");
    addPattern(grok, "cdap/grok/patterns/redis");
    addPattern(grok, "cdap/grok/patterns/ruby");
  }

  protected String determinePattern(Map<String, String> settings) {
    if (!settings.containsKey(PATTERN_SETTING)) {
      return DEFAULT_PATTERN;
    } else {
      return settings.get(PATTERN_SETTING);
    }
  }

  protected void addPattern(Grok grok, String resource) {
    URL url = this.getClass().getClassLoader().getResource(resource);
    if (url == null) {
      LOG.error("Resource '{}' for grok pattern was not found", resource);
      return;
    }

    try {
      String patternFile = Resources.toString(url, Charsets.UTF_8);
      try {
        grok.addPatternFromReader(new StringReader(patternFile));
      } catch (GrokException e) {
        LOG.error("Invalid grok pattern from resource '{}'", resource, e);
      }
    } catch (IOException e) {
      LOG.error("Failed to load resource '{}' for grok pattern", resource, e);
    }
  }

  public String getPattern() {
    return pattern;
  }
}
