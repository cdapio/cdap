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

import com.google.common.base.Stopwatch;
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
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

/**
 * Class which contains common logic about retry logic
 */
abstract class AbstractServiceRetryableMacroEvaluator implements MacroEvaluator {
  private static final long TIMEOUT_MILLIS = TimeUnit.SECONDS.toMillis(30);
  private static final long RETRY_BASE_DELAY_MILLIS = 200L;
  private static final long RETRY_MAX_DELAY_MILLIS = TimeUnit.SECONDS.toMillis(5);
  private static final double RETRY_DELAY_MULTIPLIER = 1.2d;
  private static final double RETRY_RANDOMIZE_FACTOR = 0.1d;

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
      throw new IllegalArgumentException("Invalid function name " + macroFunction
                                           + ". Expecting " + functionName);
    }

    // Make call with exponential delay on failure retry.
    long delay = RETRY_BASE_DELAY_MILLIS;
    double minMultiplier = RETRY_DELAY_MULTIPLIER - RETRY_DELAY_MULTIPLIER * RETRY_RANDOMIZE_FACTOR;
    double maxMultiplier = RETRY_DELAY_MULTIPLIER + RETRY_DELAY_MULTIPLIER * RETRY_RANDOMIZE_FACTOR;
    Stopwatch stopWatch = new Stopwatch().start();
    try {
      while (stopWatch.elapsedTime(TimeUnit.MILLISECONDS) < TIMEOUT_MILLIS) {
        try {
          return evaluateMacro(macroFunction, args);
        } catch (RetryableException e) {
          TimeUnit.MILLISECONDS.sleep(delay);
          delay = (long) (delay * (minMultiplier + Math.random() * (maxMultiplier - minMultiplier + 1)));
          delay = Math.min(delay, RETRY_MAX_DELAY_MILLIS);
        } catch (IOException e) {
          throw new RuntimeException("Failed to evaluate the macro function '" + functionName
                                       + "' with args " + Arrays.asList(args), e);
        }
      }
    } catch (InterruptedException e) {
      throw new RuntimeException("Thread interrupted while trying to evaluate the macro function '" + functionName
                                   + "' with args " + Arrays.asList(args), e);
    }
    throw new IllegalStateException("Timed out when trying to evaluate the macro function '" + functionName
                                    + "' with args " + Arrays.asList(args));
  }

  protected String validateAndRetrieveContent(String serviceName, HttpURLConnection urlConn) throws IOException {
    if (urlConn == null) {
      throw new RetryableException(serviceName + " service is not available");
    }

    int responseCode = urlConn.getResponseCode();
    if (responseCode != HttpURLConnection.HTTP_OK) {
      switch (responseCode) {
        case HttpURLConnection.HTTP_BAD_GATEWAY:
        case HttpURLConnection.HTTP_UNAVAILABLE:
        case HttpURLConnection.HTTP_GATEWAY_TIMEOUT:
          throw new RetryableException(serviceName + " service is not available with status " + responseCode);
      }
      throw new IOException("Failed to call " + serviceName + " service with status " + responseCode + ": " +
                              getError(urlConn));
    }

    try (Reader reader = new InputStreamReader(urlConn.getInputStream(), StandardCharsets.UTF_8)) {
      return CharStreams.toString(reader);
    } finally {
      urlConn.disconnect();
    }
  }

  abstract String evaluateMacro(String macroFunction,
                                String... args) throws InvalidMacroException, IOException, RetryableException;

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
