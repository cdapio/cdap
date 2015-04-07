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
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.commons.lang.StringUtils;

import java.util.concurrent.TimeUnit;

/**
 * Determines requires arguments for the adapter from the runtime arguments given to the program.
 */
public class ConversionConfig {
  private final Resources mapperResources;
  private final FormatSpecification sourceFormatSpec;
  private final Schema sinkSchema;
  private final String sourceName;
  private final String sinkName;
  private final String headers;
  private final long frequency;

  public ConversionConfig(Resources mapperResources, FormatSpecification sourceFormatSpec,
                          String sourceName, String sinkName, String headers, String frequency) {
    this.mapperResources = mapperResources;
    this.sourceFormatSpec = sourceFormatSpec;
    this.sourceName = sourceName;
    this.sinkName = sinkName;
    this.headers = headers;
    this.frequency = parseFrequency(frequency);
    Schema sourceSchema = new Schema.Parser().parse(sourceFormatSpec.getSchema().toString());
    SchemaBuilder.FieldAssembler<Schema> fieldAssembler = SchemaBuilder.builder().record("streamEvent").fields()
      .requiredLong("ts");
    for (String header : headers.split(",")) {
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

  public String getHeaders() {
    return headers;
  }

  public Schema getSinkSchema() {
    return sinkSchema;
  }

  public long getFrequency() {
    return frequency;
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
