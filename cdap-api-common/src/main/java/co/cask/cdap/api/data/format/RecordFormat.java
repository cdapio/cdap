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

package co.cask.cdap.api.data.format;

import co.cask.cdap.api.annotation.Beta;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.data.schema.UnsupportedTypeException;

import java.util.Collections;
import java.util.Map;

/**
 * Interface specifying how to read data in some format into java objects.
 * A format implies at least a default schema, which may be as simple as a byte array.
 *
 * @param <FROM> the raw data to read from.
 * @param <TO> object to format the data into.
 */
@Beta
public abstract class RecordFormat<FROM, TO> {
  protected Schema schema;

  /**
   * Read data from the input format to the output type.
   *
   * @param input input object to read.
   * @return formatted input.
   * @throws UnexpectedFormatException if the input object could not be read because it is of an unexpected format.
   */
  public abstract TO read(FROM input) throws UnexpectedFormatException;

  /**
   * Get the default schema for the format. The default is used if no schema is provided in the call
   * to {@link #initialize(FormatSpecification)}. Should return null if there is no default schema, meaning
   * a schema must be provided during initialization of the format.
   *
   * @return the default schema for the format, or null if a schema must be provided to the format
   */
  protected abstract Schema getDefaultSchema();

  /**
   * Validate the given schema, throwing an exception if it is unsupported. It can be assumed that the input schema
   * is not null and is a record of at least one field.
   *
   * @param schema the schema to validate for the format
   * @throws UnsupportedTypeException if the schema not supported
   */
  protected abstract void validateSchema(Schema schema) throws UnsupportedTypeException;

  /**
   * Initialize the format with the given desired schema and properties.
   * Guaranteed to be called once before any other method is called.
   *
   * @param formatSpecification the specification for the format, containing the desired schema and settings
   * @throws UnsupportedTypeException if the desired schema and properties are not supported
   */
  public void initialize(FormatSpecification formatSpecification) throws UnsupportedTypeException {
    Schema desiredSchema = null;
    Map<String, String> settings = Collections.emptyMap();
    if (formatSpecification != null) {
      desiredSchema = formatSpecification.getSchema();
      settings = formatSpecification.getSettings();
    }
    desiredSchema = desiredSchema == null ? getDefaultSchema() : desiredSchema;
    if (desiredSchema == null) {
      throw new UnsupportedTypeException("A schema must be provided to this format.");
    }
    validateIsRecord(desiredSchema);
    validateSchema(desiredSchema);
    this.schema = desiredSchema;
    configure(settings);
  }

  /**
   * Configure the format with the given properties. Guaranteed to be called once before any call to
   * {@link #read(Object)} is made, and after a schema for the format has been set.
   *
   * @param settings the settings to configure the format with
   */
  protected void configure(Map<String, String> settings) {
    // do nothing by default
  }


  /**
   * Get the schema of the format.
   *
   * @return the schema of the format.
   */
  public Schema getSchema() {
    return schema;
  }

  private void validateIsRecord(Schema schema) throws UnsupportedTypeException {
    if (schema.getType() != Schema.Type.RECORD || schema.getFields().size() < 1) {
      throw new UnsupportedTypeException("Schema must be a record with at least one field.");
    }
  }
}
