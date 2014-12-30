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

package co.cask.cdap.internal.format;

import co.cask.cdap.internal.io.Schema;
import co.cask.cdap.internal.io.UnsupportedTypeException;
import co.cask.cdap.internal.specification.FormatSpecification;

import java.util.Map;

/**
 * Interface specifying how to read data in some format into java objects.
 * A format implies at least a default schema, which may be as simple as a byte array.
 *
 * @param <FROM> the raw data to read from.
 * @param <TO> object to format the data into.
 */
public abstract class RecordFormat<FROM, TO> {
  protected Schema schema;

  protected RecordFormat() {
    this.schema = getDefaultSchema();
  }

  /**
   * Read data from the input format to the output type.
   *
   * @param input input object to read.
   * @return formatted input.
   * @throws UnexpectedFormatException if the input object could not be read because it is of an unexpected format.
   */
  public abstract TO read(FROM input) throws UnexpectedFormatException;

  /**
   * Get the default schema for the format.
   *
   * @return default schema for the format.
   */
  protected abstract Schema getDefaultSchema();

  /**
   * Validate the desired schema, throwing an exception if it is unsupported. It can be assumed that the input schema
   * is not null and is a record of at least one field.
   *
   * @param desiredSchema desired schema for the format.
   * @throws UnsupportedTypeException if the desired schema not supported.
   */
  protected abstract void validateDesiredSchema(Schema desiredSchema) throws UnsupportedTypeException;

  /**
   * Configure the format with the given properties. Guaranteed to be called once before any call to
   * {@link #read(Object)} is made.
   *
   * @param settings
   */
  protected abstract void configure(Map<String, String> settings);

  /**
   * Initialize the format with the given desired schema and properties.
   * Guaranteed to be called once before any other method is called.
   *
   * @param formatSpecification specification for the format, containing the desired schema and settings.
   * @throws UnsupportedTypeException if the desired schema and properties are not supported.
   */
  public void initialize(FormatSpecification formatSpecification)
    throws UnsupportedTypeException {
    Schema desiredSchema = formatSpecification.getSchema();
    if (desiredSchema != null) {
      validateIsRecord(desiredSchema);
      validateDesiredSchema(desiredSchema);
      this.schema = desiredSchema;
    }
    configure(formatSpecification.getSettings());
  }

  /**
   * Get the schema of the format.
   *
   * @return schema of the format.
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
