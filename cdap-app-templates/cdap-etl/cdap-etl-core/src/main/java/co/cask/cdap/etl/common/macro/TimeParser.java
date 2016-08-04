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

package co.cask.cdap.etl.common.macro;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;

import java.util.concurrent.TimeUnit;

/**
 * Parses time expressions and does simple time math.
 */
public class TimeParser {
  private final long runtime;

  public TimeParser(long runtime) {
    this.runtime = runtime;
  }

  /**
   * Parses time strings into timestamps, with support for some basic math.
   * Math syntax includes addition and subtraction in seconds, minutes, hours, and days.
   * Periods of time are specified as some number followed by the time unit,
   * where 's' is seconds', 'm' is minutes, 'h' is hours, and 'd' is days.
   * The "runtime" keyword translates to some given runtime, and can be strung together with
   * various additions and subtractions.
   * For example, "runtime-5s" is 5 seconds before runtime, "runtime-1d" is one day before runtime,
   * and "runtime-1d+4h" is 20 hours before runtime.
   *
   * @param str the time string to parse
   * @return milliseconds equivalent of the time string
   */
  public long parseRuntime(String str) {
    MathNode mathNode = parseExpression(str);
    return mathNode.evaluate();
  }

  /**
   * Parses a duration String to its long value.
   * Frequency string consists of a number followed by an unit, with 's' for seconds, 'm' for minutes, 'h' for hours
   * and 'd' for days. For example, an input of '5m' means 5 minutes which will be parsed to 300000 milliseconds.
   *
   * @param durationStr the duration string (ex: 5m, 5h etc).
   * @return milliseconds equivalent of the duration string
   */
  public static long parseDuration(String durationStr) {
    Preconditions.checkArgument(!Strings.isNullOrEmpty(durationStr));
    durationStr = durationStr.trim().toLowerCase();

    String value = durationStr.substring(0, durationStr.length() - 1);
    long parsedValue = Long.parseLong(value);

    char lastChar = durationStr.charAt(durationStr.length() - 1);
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

  // parse an expression into an evaluation tree
  private MathNode parseExpression(String expression) {
    expression = expression.trim();
    // we only support + and - right now so just evaluate from left to right
    int idx = Math.max(expression.lastIndexOf('+'), expression.lastIndexOf('-'));

    // if no operator, this should be a duration string
    if (idx < 0) {
      return "runtime".equals(expression) ? new ValueNode(runtime) : new ValueNode(parseDuration(expression));
    }

    char operator = expression.charAt(idx);
    if (idx == expression.length()) {
      throw new IllegalArgumentException(
        String.format("Invalid expression '%s'. Cannot end with %s.", expression, operator));
    }

    // if this in an expression like '-5d'
    if (idx == 0) {
      return new ValueNode(parseDuration(expression));
    }

    MathNode left = parseExpression(expression.substring(0, idx));
    MathNode right = parseExpression(expression.substring(idx + 1));
    if (operator == '+') {
      return new AddNode(left, right);
    }
    return new SubtractNode(left, right);
  }

  private interface MathNode {
    long evaluate();
  }

  private static class ValueNode implements MathNode {
    private long value;

    ValueNode(long value) {
      this.value = value;
    }

    public long evaluate() {
      return value;
    }
  }

  private static class AddNode implements MathNode {
    private final MathNode left;
    private final MathNode right;

    AddNode(MathNode left, MathNode right) {
      this.left = left;
      this.right = right;
    }

    @Override
    public long evaluate() {
      return left.evaluate() + right.evaluate();
    }
  }

  private static class SubtractNode implements MathNode {
    private final MathNode left;
    private final MathNode right;

    SubtractNode(MathNode left, MathNode right) {
      this.left = left;
      this.right = right;
    }

    @Override
    public long evaluate() {
      return left.evaluate() - right.evaluate();
    }
  }
}
