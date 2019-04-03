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

package co.cask.cdap.logging.filter;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;

import java.io.IOException;
import java.io.StreamTokenizer;
import java.io.StringReader;

/**
 * Parses string expression into a @{link Filter}.
 */
public final class FilterParser {
  private enum Operator {
    AND, OR
  }

  public static Filter parse(String expression) {
    StreamTokenizer tokenizer = new StreamTokenizer(new StringReader(expression));
    // Treat : as part of word
    tokenizer.wordChars((int) ':', (int) ':');

    // Treat End of Line as whitespace
    tokenizer.eolIsSignificant(false);

    // Reset special handling of numerals
    tokenizer.ordinaryChars((int) '0', (int) '9');
    tokenizer.ordinaryChar((int) '.');
    tokenizer.ordinaryChar((int) '-');
    tokenizer.wordChars((int) '0', (int) '9');
    tokenizer.wordChars((int) '.', (int) '.');

    // Check if empty expression
    try {
      tokenizer.nextToken();
      if (tokenizer.ttype == StreamTokenizer.TT_EOF) {
        return Filter.EMPTY_FILTER;
      } else if (tokenizer.ttype == (int) '\'' || tokenizer.ttype == (int) '"') {
        // Empty quoted string - '' or ""
        if (tokenizer.sval.isEmpty()) {
          return Filter.EMPTY_FILTER;
        }
      }
    } catch (IOException e) {
      throw Throwables.propagate(e);
    }

    // Not an empty expression
    tokenizer.pushBack();
    return parseExpression(tokenizer);
  }

  private static Filter parseExpression(StreamTokenizer tokenizer) {
    Filter operand1 = parseOperand(tokenizer);

    Operator operator = parseOperator(tokenizer);
    if (operator == null) {
      return operand1;
    }

    Filter operand2 = parseExpression(tokenizer);

    return createFilter(operand1, operand2, operator);
  }

  private static Filter parseOperand(StreamTokenizer tokenizer) {
    String key = parseString(tokenizer);
    parseEquals(tokenizer);
    String value = parseString(tokenizer);

    // Generate expression
    if (key.startsWith(".")) {
      // System tag
      return new MdcExpression(key, value);
    } else if (key.startsWith("MDC:")) {
      // User MDC tag
      return new MdcExpression(key, value);
    } else if (key.equals("loglevel")) {
      // Log level
      return new LogLevelExpression(value);
    } else {
      throw new IllegalArgumentException(String.format("Unknown expression of type %s", key));
    }
  }

  private static String parseString(StreamTokenizer tokenizer) {
    try {
      tokenizer.nextToken();
      if (tokenizer.ttype != StreamTokenizer.TT_EOF) {
        switch (tokenizer.ttype) {
          // handle operands
          case (int) '\'':
          case (int) '"':
          case StreamTokenizer.TT_WORD:
            return tokenizer.sval;

          default:
            throw new IllegalStateException(String.format("Expected operand but got %s", (char) tokenizer.ttype));
        }
      }
    } catch (IOException e) {
      throw Throwables.propagate(e);
    }
    throw new IllegalStateException("Expected operand but got end of expression");
  }

  private static void parseEquals(StreamTokenizer tokenizer) {
    try {
      tokenizer.nextToken();
      if (tokenizer.ttype != StreamTokenizer.TT_EOF) {
        switch (tokenizer.ttype) {
          // handle equals
          case (int) '=':
            return;

          default:
            throw new IllegalStateException(String.format("Expected operator = but got %s", (char) tokenizer.ttype));
        }
      }
    } catch (IOException e) {
      throw Throwables.propagate(e);
    }
    throw new IllegalStateException("Expected operator = but got end of expression");
  }

  private static Operator parseOperator(StreamTokenizer tokenizer) {
    try {
      tokenizer.nextToken();
      if (tokenizer.ttype != StreamTokenizer.TT_EOF) {
        switch (tokenizer.ttype) {
          // handle operator
          case StreamTokenizer.TT_WORD:
            return Operator.valueOf(tokenizer.sval);

          default:
            throw new IllegalStateException(String.format("Expected operator = but got %s", (char) tokenizer.ttype));
        }
      }
    } catch (IOException e) {
      throw Throwables.propagate(e);
    }

    // No operator present
    return null;
  }

  private static Filter createFilter(Filter operand1, Filter operand2, Operator operator) {
    if (operator == null) {
      return Filter.EMPTY_FILTER;
    }

    switch (operator) {
      case AND:
        return new AndFilter(ImmutableList.of(operand1, operand2));
      case OR:
        return new OrFilter(ImmutableList.of(operand1, operand2));
      default:
        throw new UnsupportedOperationException(String.format("Operator %s not supported", operator));
    }
  }
}
