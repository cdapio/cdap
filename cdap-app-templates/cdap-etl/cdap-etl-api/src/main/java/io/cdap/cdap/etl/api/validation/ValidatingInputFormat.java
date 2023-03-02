/*
 * Copyright © 2019 Cask Data, Inc.
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

package io.cdap.cdap.etl.api.validation;

import io.cdap.cdap.api.data.batch.InputFormatProvider;
import io.cdap.cdap.api.data.schema.Schema;
import java.io.IOException;
import javax.annotation.Nullable;

/**
 * Validating input format provider.
 */
public interface ValidatingInputFormat extends InputFormatProvider {

  String PLUGIN_TYPE = "validatingInputFormat";

  /**
   * Validates configurations of input format.
   *
   * @param context format context
   */
  void validate(FormatContext context);

  /**
   * Gets validated schema.
   *
   * @param context format context
   */
  @Nullable
  Schema getSchema(FormatContext context);

  /**
   * Try to detect the schema from some input data.
   *
   * @param context format context
   * @param inputFiles files to sample from in order to detect schema.
   * @return the detected schema, or null if the schema could not be detected.
   * @throws IOException if there was an issue opening or reading the data.
   */
  @Nullable
  default Schema detectSchema(FormatContext context, InputFiles inputFiles) throws IOException {
    return getSchema(context);
  }
}
