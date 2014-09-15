/*
 * Copyright 2012-2014 Cask, Inc.
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

package co.cask.cdap.shell;

import com.google.common.collect.ImmutableMap;

/**
 *
 */
public class CommandPattern {

  private final String pattern;

  public CommandPattern(String pattern) {
    this.pattern = pattern;
  }

  public Arguments parseArguments(String input) {
    String[] patternTokens = pattern.split(" ");
    String[] inputTokens = input.split(" ");

    ImmutableMap.Builder<String, String> args = ImmutableMap.builder();

    for (int i = 0; i < patternTokens.length; i++) {
      String patternToken = patternTokens[i];
      if (patternToken.startsWith("<") && patternToken.endsWith(">")) {
        // required argument
        args.put(patternToken.substring(1, patternToken.length() - 1), inputTokens[i]);
      } else if (patternToken.startsWith("[") && patternToken.endsWith("]")) {
        // optional argument
        if (i < inputTokens.length) {
          args.put(patternToken.substring(1, patternToken.length() - 1), inputTokens[i]);
        }
      } else {
        if (!patternToken.equals(inputTokens[i])) {
          throw new IllegalArgumentException("Expected format: " + pattern);
        }
      }
    }

    return new Arguments(args.build(), input);
  }
}
