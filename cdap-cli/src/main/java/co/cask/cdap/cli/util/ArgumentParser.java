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

package co.cask.cdap.cli.util;

import co.cask.cdap.cli.ProgramIdArgument;
import co.cask.common.cli.util.Parser;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static co.cask.common.cli.util.Parser.MANDATORY_ARG_BEGINNING;
import static co.cask.common.cli.util.Parser.MANDATORY_ARG_ENDING;
import static co.cask.common.cli.util.Parser.OPTIONAL_PART_BEGINNING;
import static co.cask.common.cli.util.Parser.OPTIONAL_PART_ENDING;

/**
 * Utility class for parsing arguments from user input based on command pattern.
 */
public class ArgumentParser {

  /**
   * Parses a map in the format: "key1=a, key2=b, .."
   *
   * @param mapString {@link String} representation of the map
   * @return the map
   */
  public static Map<String, String> parseMap(String mapString) {
    if (mapString == null || mapString.isEmpty()) {
      return ImmutableMap.of();
    }
    return Splitter.on(",").omitEmptyStrings().trimResults().withKeyValueSeparator("=").split(mapString);
  }

  /**
   * Parses program id from user input.
   *
   * @param programId the program id
   * @return ProgramIdArgument
   */
  public static ProgramIdArgument parseProgramId(String programId) {
    if (programId == null) {
      return null;
    }
    String[] split = programId.split("\\.");
    if (split.length != 2) {
      return null;
    }
    return new ProgramIdArgument(split[0], split[1]);
  }

  /**
   * Parses params from user input.
   *
   * @param input user input to be parsed
   * @param pattern pattern by which to parse
   * @return parsed params
   */
  public static Map<String, String> getArguments(String input, String pattern) {
    if (input == null || pattern == null) {
      return Collections.emptyMap();
    }
    input = cutNotFullyEnteredToken(input);
    Map<String, String> arguments = Maps.newHashMap();
    List<String> patternTokens = Parser.parsePattern(pattern);
    List<String> inputTokens = Parser.parseInput(input);
    while (!patternTokens.isEmpty() && !inputTokens.isEmpty()) {
      String patternPart = patternTokens.get(0);
      String inputPart = inputTokens.get(0);
      if (patternPart.startsWith((Character.toString(OPTIONAL_PART_BEGINNING))) &&
        patternPart.endsWith((Character.toString(OPTIONAL_PART_ENDING)))) {
        arguments.putAll(parseOptional(inputTokens, getEntry(patternPart)));
      } else {
        if (patternPart.startsWith((Character.toString(MANDATORY_ARG_BEGINNING))) &&
          patternPart.endsWith((Character.toString(MANDATORY_ARG_ENDING)))) {
          arguments.put(getEntry(patternPart), tryGetInputEntry(inputPart));
        } else if (!patternPart.equals(inputPart)) {
          return Collections.emptyMap();
        }
        inputTokens.remove(0);
      }
      patternTokens.remove(0);
    }
    return arguments;
  }

  /**
   * Parses params from optional part.
   *
   * @param splitInput optional part to be parsed
   * @param pattern pattern by which to parse
   * @return parsed params
   */
  private static Map<String, String> parseOptional(List<String> splitInput, String pattern) {
    ImmutableMap.Builder<String, String> args = ImmutableMap.builder();

    List<String> copyInput = new ArrayList<String>(splitInput);
    List<String> splitPattern = Parser.parsePattern(pattern);

    while (!splitPattern.isEmpty()) {
      if (copyInput.isEmpty()) {
        return Collections.emptyMap();
      }
      String patternPart = splitPattern.get(0);
      String inputPart = tryGetInputEntry(copyInput.get(0));
      if (patternPart.startsWith((Character.toString(MANDATORY_ARG_BEGINNING))) &&
        patternPart.endsWith((Character.toString(MANDATORY_ARG_ENDING)))) {
        args.put(getEntry(patternPart), inputPart);
      } else if (patternPart.startsWith((Character.toString(OPTIONAL_PART_BEGINNING))) &&
        patternPart.endsWith((Character.toString(OPTIONAL_PART_ENDING)))) {
        args.putAll(parseOptional(copyInput, getEntry(patternPart)));
      } else if (!patternPart.equals(inputPart)) {
        return Collections.emptyMap();
      }
      splitPattern.remove(0);
      copyInput.remove(0);
    }

    splitInput.clear();
    splitInput.addAll(copyInput);
    return args.build();
  }

  /**
   * Cuts last not fully entered token,
   * where token is a word or some expression in quotes or double quotes.
   *
   * @param input the input to cut
   * @return cutted input
   */
  private static String cutNotFullyEnteredToken(String input) {
    int index = input.lastIndexOf(" ");
    return input.substring(0, index == -1 ? input.length() : index);
  }

  /**
   * Retrieves entry from input {@link String}.
   *
   * @param input the input
   * @return entry {@link String}
   */
  private static String tryGetInputEntry(String input) {
    if (input.startsWith("'") && input.endsWith("'") ||
      input.startsWith("\"") && input.endsWith("\"")) {
      return getEntry(input);
    }
    return input;
  }

  /**
   * Retrieves entry from input {@link String}.
   * For example, for input "<some input>" returns "some input".
   *
   * @param input the input
   * @return entry {@link String}
   */
  private static String getEntry(String input) {
    return input.substring(1, input.length() - 1);
  }
}
