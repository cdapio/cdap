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

package co.cask.cdap.examples.streamconversion.app;

import co.cask.cdap.api.Resources;
import co.cask.cdap.api.data.format.FormatSpecification;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Sets;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.commons.lang.StringUtils;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Determines requires arguments for the adapter from the runtime arguments given to the program.
 */
public class AdapterArguments {
  private static final Type MAP_TYPE = new TypeToken<Map<String, String>>() { }.getType();
  private static final Gson GSON = new Gson();
  // keys for adapter level args
  public static final String FREQUENCY = "frequency";
  public static final String MAPPER_MEMORY = "mapper.resources.memory.mb";
  public static final String MAPPER_VCORES = "mapper.resources.vcores";
  public static final String SINK_NAME = "sink.name";
  public static final String SOURCE_NAME = "source.name";
  // keys for source level args
  public static final String FORMAT_NAME = "source.format.name";
  public static final String FORMAT_SETTINGS = "source.format.settings";
  public static final String SCHEMA = "source.schema";
  public static final String HEADERS = "headers";

  // required args
  private final Resources mapperResources;
  private final FormatSpecification sourceFormatSpec;
  private final Set<String> headers;
  private final Schema sinkSchema;
  private final String sourceName;
  private final String sinkName;
  private final String headersStr;
  private final long frequency;

  public AdapterArguments(Map<String, String> runtimeArgs) throws IOException {
    this.sourceName = getRequired(runtimeArgs, SOURCE_NAME);
    this.sinkName = getRequired(runtimeArgs, SINK_NAME);
    this.frequency = parseFrequency(getRequired(runtimeArgs, FREQUENCY));
    int mapperMemoryMB = get(runtimeArgs, MAPPER_MEMORY, 512);
    int mapperVirtualCores = get(runtimeArgs, MAPPER_VCORES, 1);
    this.mapperResources = new Resources(mapperMemoryMB, mapperVirtualCores);

    // get source level args
    String formatName = getRequired(runtimeArgs, FORMAT_NAME);
    Map<String, String> formatSettings = GSON.fromJson(get(runtimeArgs, FORMAT_SETTINGS, "{}"), MAP_TYPE);
    String schemaStr = getRequired(runtimeArgs, SCHEMA);
    co.cask.cdap.api.data.schema.Schema bodySchema = co.cask.cdap.api.data.schema.Schema.parseJson(schemaStr);
    this.sourceFormatSpec = new FormatSpecification(formatName, bodySchema, formatSettings);
    this.headersStr = runtimeArgs.get(HEADERS);
    this.headers = Sets.newHashSet();
    if (headersStr != null) {
      for (String header : headersStr.split(",")) {
        this.headers.add(header);
      }
    }

    // get sink level args
    // sink schema is the source schema plus timestamp and headers
    Schema sourceSchema = new Schema.Parser().parse(schemaStr);
    SchemaBuilder.FieldAssembler<Schema> fieldAssembler = SchemaBuilder.builder().record("streamEvent").fields()
      .requiredLong("ts");
    for (String header : headers) {
      fieldAssembler.optionalString(header);
    }
    for (Schema.Field field : sourceSchema.getFields()) {
      fieldAssembler.name(field.name()).type(field.schema()).noDefault();
    }
    this.sinkSchema = fieldAssembler.endRecord();
  }

  public FormatSpecification getSourceFormatSpec() {
    return sourceFormatSpec;
  }

  public Resources getMapperResources() {
    return mapperResources;
  }

  public String getSourceName() {
    return sourceName;
  }

  public String getSinkName() {
    return sinkName;
  }

  public String getHeadersStr() {
    return headersStr;
  }

  public Schema getSinkSchema() {
    return sinkSchema;
  }

  public long getFrequency() {
    return frequency;
  }

  private String get(Map<String, String> map, String key, String defaultValue) {
    String val = map.get(key);
    return val == null ? defaultValue : val;
  }

  private int get(Map<String, String> map, String key, Integer defaultValue) {
    String val = map.get(key);
    return val == null ? defaultValue : Integer.parseInt(val);
  }

  private String getRequired(Map<String, String> map, String key) {
    String val = map.get(key);
    if (val == null) {
      throw new IllegalArgumentException(key + " is missing.");
    }
    return val;
  }

  private long parseFrequency(String frequencyStr) {
    //TODO: replace with TimeMathParser (available only internal to cdap)
    Preconditions.checkArgument(!Strings.isNullOrEmpty(frequencyStr));
    frequencyStr = frequencyStr.toLowerCase();

    String value = frequencyStr.substring(0, frequencyStr.length() - 1);
    Preconditions.checkArgument(StringUtils.isNumeric(value));
    Integer parsedValue = Integer.valueOf(value);
    Preconditions.checkArgument(parsedValue > 0);

    char lastChar = frequencyStr.charAt(frequencyStr.length() - 1);
    switch (lastChar) {
      case 's':
        return TimeUnit.SECONDS.toMillis(parsedValue);
      case 'm':
        return TimeUnit.MINUTES.toMillis(parsedValue);
      case 'h':
        return TimeUnit.HOURS.toMillis(parsedValue);
      case 'd':
        return TimeUnit.DAYS.toMillis(parsedValue);
    }
    throw new IllegalArgumentException(String.format("Time unit not supported: %s", lastChar));
  }
}
