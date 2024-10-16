/*
 * Copyright © 2024 Cask Data, Inc.
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

package io.cdap.cdap.etl.common;

import io.cdap.cdap.api.exception.ProgramFailureException;
import io.cdap.cdap.api.exception.WrappedStageException;
import io.cdap.cdap.etl.api.exception.ErrorContext;
import io.cdap.cdap.etl.api.exception.ErrorDetailsProvider;
import io.cdap.cdap.etl.api.exception.ErrorPhase;
import io.cdap.cdap.etl.api.exception.NoopErrorDetailsProvider;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility class for handling exceptions.
 */
public class ErrorDetails {
  private static final Logger LOG = LoggerFactory.getLogger(ErrorDetails.class);
  public static final String ERROR_DETAILS_PROVIDER_CLASS_NAME_KEY =
      "io.cdap.pipeline.error.details.provider.classname";

  /**
   * Gets the {@link ErrorDetailsProvider} from the given {@link Configuration}.
   *
   * @param conf the configuration to get the error details provider from.
   * @return the error details provider.
   */
  public static ErrorDetailsProvider getErrorDetailsProvider(Configuration conf) {
    String errorDetailsProviderClassName =
      conf.get(ERROR_DETAILS_PROVIDER_CLASS_NAME_KEY);
    if (errorDetailsProviderClassName == null) {
      return new NoopErrorDetailsProvider();
    }
    try {
      return (ErrorDetailsProvider) conf.getClassLoader()
        .loadClass(errorDetailsProviderClassName)
        .newInstance();
    } catch (Exception e) {
      LOG.warn(String.format("Unable to instantiate errorDetailsProvider class '%s'.",
          errorDetailsProviderClassName), e);
      return new NoopErrorDetailsProvider();
    }
  }

  /**
   * Handles the given exception, wrapping it in a {@link WrappedStageException}.
   *
   * @param e the exception to handle.
   * @param stageName the name of the stage where the exception occurred.
   * @param errorDetailsProvider the error details provider.
   * @param phase the phase of the stage where the exception occurred.
   * @return the wrapped stage exception.
   */
  public static WrappedStageException handleException(Exception e, String stageName,
    ErrorDetailsProvider errorDetailsProvider, ErrorPhase phase) {
    ProgramFailureException exception = null;

    if (!(e instanceof ProgramFailureException)) {
      exception = errorDetailsProvider == null ? null :
        errorDetailsProvider.getExceptionDetails(e, new ErrorContext(phase));
    }
    return new WrappedStageException(exception == null ? e : exception, stageName);
  }
}
