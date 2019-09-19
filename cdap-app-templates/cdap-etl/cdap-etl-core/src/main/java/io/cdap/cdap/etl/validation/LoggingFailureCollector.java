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

package io.cdap.cdap.etl.validation;

import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.validation.ValidationException;
import io.cdap.cdap.etl.api.validation.ValidationFailure;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Failure collector that logs the failures.
 */
public class LoggingFailureCollector extends DefaultFailureCollector {
  private static final Logger LOG = LoggerFactory.getLogger(LoggingFailureCollector.class);

  /**
   * Failure collector that logs the failures.
   *
   * @param stageName stage name
   * @param inputSchemas input schemas
   */
  public LoggingFailureCollector(String stageName, Map<String, Schema> inputSchemas) {
    super(stageName, inputSchemas);
  }

  @Override
  public ValidationException getOrThrowException() throws ValidationException {
    ValidationException validationException;
    try {
      validationException = super.getOrThrowException();
    } catch (ValidationException e) {
      validationException = e;
    }

    if (validationException.getFailures().isEmpty()) {
      return validationException;
    }

    List<ValidationFailure> failures = validationException.getFailures();
    LOG.error("Encountered '{}' validation failures: {}{}", failures.size(), System.lineSeparator(),
              IntStream.range(0, failures.size())
                .mapToObj(index -> String.format("%d. '%s'", index + 1, failures.get(index).getFullMessage()))
                .collect(Collectors.joining(System.lineSeparator())));

    throw validationException;
  }
}
