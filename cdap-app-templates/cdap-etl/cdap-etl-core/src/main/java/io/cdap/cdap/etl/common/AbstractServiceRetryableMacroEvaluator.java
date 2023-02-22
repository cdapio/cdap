/*
 * Copyright © 2021 Cask Data, Inc.
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
 *
 */

package io.cdap.cdap.etl.common;

import com.google.common.io.ByteStreams;
import com.google.common.io.CharStreams;
import io.cdap.cdap.api.macro.InvalidMacroException;
import io.cdap.cdap.api.macro.MacroEvaluator;
import io.cdap.cdap.api.retry.RetryableException;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.HttpURLConnection;
import java.nio.charset.StandardCharsets;
import java.util.Map;

/**
 * Class which contains common logic about retry logic
 */
abstract class AbstractServiceRetryableMacroEvaluator implements MacroEvaluator {

  private final String functionName;

  AbstractServiceRetryableMacroEvaluator(String functionName) {
    this.functionName = functionName;
  }

  @Override
  public String lookup(String property) throws InvalidMacroException {
    throw new InvalidMacroException("The '" + functionName
                                      + "' macro function doesn't support direct property lookup for property '"
                                      + property + "'");
  }

  @Override
  public String evaluate(String macroFunction, String... args) throws InvalidMacroException {
    if (!functionName.equals(macroFunction)) {
      // This shouldn't happen
      throw new IllegalArgumentException("Invalid function name " + macroFunction + ". Expecting " + functionName);
    }

    RetryableBiFunction<String, String[], String> retryingEvaluateStringFunc =
      new RetryableBiFunction<String, String[], String>() {
      @Override
      public String apply(String macroFunction, String[] args) throws Throwable {
        return evaluateMacro(macroFunction, args);
      }
    };

    return retryingEvaluateStringFunc.applyWithRetries(macroFunction, macroFunction, args);
  }

  @Override
  public Map<String, String> evaluateMap(String macroFunction, String... args) throws InvalidMacroException {
    if (!functionName.equals(macroFunction)) {
      // This shouldn't happen
      throw new IllegalArgumentException("Invalid function name " + macroFunction
                                           + ". Expecting " + functionName);
    }

    RetryableBiFunction<String, String[], Map<String, String>> retryingEvaluateMapFunc =
      new RetryableBiFunction<String, String[], Map<String, String>>() {
      @Override
      public Map<String, String> apply(String macroFunction, String[] args) throws Throwable {
        return evaluateMacroMap(macroFunction, args);
      }
    };

    return retryingEvaluateMapFunc.applyWithRetries(macroFunction, macroFunction, args);
  }

  protected String validateAndRetrieveContent(String serviceName,
                                              HttpURLConnection urlConn) throws IOException {
    if (urlConn == null) {
      throw new RetryableException(serviceName + " service is not available");
    }
    validateResponseCode(serviceName, urlConn);
    try (Reader reader = new InputStreamReader(urlConn.getInputStream(), StandardCharsets.UTF_8)) {
      return CharStreams.toString(reader);
    } catch (IOException e) {
      // IOExceptions are retryable for idempotent operations.
      throw new RetryableException(e);
    } finally {
      urlConn.disconnect();
    }
  }

  private void validateResponseCode(String serviceName, HttpURLConnection urlConn) throws IOException {
    int responseCode;
    try {
      responseCode = urlConn.getResponseCode();
    } catch (IOException e) {
      // IOExceptions are retryable for idempotent operations.
      throw new RetryableException(e);
    }
    if (responseCode != HttpURLConnection.HTTP_OK) {
      if (HttpCodes.isRetryable(responseCode)) {
        throw new RetryableException(serviceName + " service is not available with status " + responseCode);
      }
      throw new IOException("Failed to call " + serviceName + " service with status " + responseCode + ": " +
                              getError(urlConn));
    }
  }

  abstract Map<String, String> evaluateMacroMap(
    String macroFunction, String... args) throws InvalidMacroException, IOException, RetryableException;

  abstract String evaluateMacro(
      String macroFunction, String... args) throws InvalidMacroException, IOException, RetryableException;

  /**
   * Returns the full content of the error stream for the given {@link HttpURLConnection}.
   */
  private String getError(HttpURLConnection urlConn) {
    try (InputStream is = urlConn.getErrorStream()) {
      if (is == null) {
        return "Unknown error";
      }
      return new String(ByteStreams.toByteArray(is), StandardCharsets.UTF_8);
    } catch (IOException e) {
      return "Unknown error due to failure to read from error output: " + e.getMessage();
    }
  }
}
