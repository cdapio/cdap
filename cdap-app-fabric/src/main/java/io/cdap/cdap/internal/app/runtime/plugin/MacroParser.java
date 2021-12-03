/*
 * Copyright © 2016-2019 Cask Data, Inc.
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

package io.cdap.cdap.internal.app.runtime.plugin;

import com.google.common.base.Splitter;
import com.google.common.collect.Iterables;
import com.google.gson.Gson;
import io.cdap.cdap.api.macro.InvalidMacroException;
import io.cdap.cdap.api.macro.MacroEvaluator;
import io.cdap.cdap.api.macro.MacroObjectType;
import io.cdap.cdap.api.macro.MacroParserOptions;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;
import javax.annotation.Nullable;

/**
 * Provides a parser to validate the syntax of and evaluate macros.
 */
public class MacroParser {
  private static final Pattern ARGUMENT_DELIMITER = Pattern.compile(",");
  private static final int MAX_SUBSTITUTION_DEPTH = 10;
  private static final Gson GSON = new Gson();

  private final MacroEvaluator macroEvaluator;
  private final boolean escapingEnabled;
  private final boolean lookupsEnabled;
  private final boolean functionsEnabled;
  private final boolean skipInvalid;
  private final Set<String> functionWhitelist;

  public MacroParser(MacroEvaluator macroEvaluator) {
    this(macroEvaluator, MacroParserOptions.DEFAULT);
  }

  public MacroParser(MacroEvaluator macroEvaluator, MacroParserOptions options) {
    this.macroEvaluator = macroEvaluator;
    this.escapingEnabled = options.isEscapingEnabled();
    this.lookupsEnabled = options.shouldEvaluateLookups();
    this.functionsEnabled = options.shouldEvaluateFunctions();
    this.functionWhitelist = options.getFunctionWhitelist();
    this.skipInvalid = options.shouldSkipInvalid();
  }

  /**
   * Substitutes the provided string with a given macro evaluator. Expands macros from right-to-left recursively.
   * If the string passed to parse is null, then null will be returned.
   * @param str the raw string containing macro syntax
   * @return the original string with all macros expanded and substituted
   * @throws InvalidMacroException
   */
  @Nullable
  public String parse(@Nullable String str) throws InvalidMacroException {
    if (str == null) {
      return null;
    }
    // final string should have escapes that are not directly embedded in macro syntax replaced
    return replaceEscapedSyntax(parse(str, 0));
  }

  /**
   * Recursive helper method for macro parsing. Expands macros from right-to-left up to a maximum depth.
   * @param str the raw string containing macro syntax
   * @param depth the current depth of expansion
   * @return the parsed string
   * @throws InvalidMacroException
   */
  private String parse(String str, int depth) throws InvalidMacroException {
    if (depth > MAX_SUBSTITUTION_DEPTH) {
      throw new InvalidMacroException(String.format("Failed substituting macro '%s', expansion exceeded %d levels.",
                                                    str, MAX_SUBSTITUTION_DEPTH));
    }

    MacroMetadata macroPosition = findRightmostMacro(str, str.length());
    while (macroPosition != null) {
      // in the case that the MacroEvaluator does not internally handle unspecified macros
      if (macroPosition.substitution == null) {
        throw new InvalidMacroException(String.format("Unable to substitute macro in string %s.", str));
      }
      String left = str.substring(0, macroPosition.startIndex);
      String right = str.substring(macroPosition.endIndex + 1);
      String middle = macroPosition.substitution;
      if (macroPosition.evaluateRecursively) {
        middle = parse(macroPosition.substitution, depth + 1);
      }
      str = left + middle + right;
      // if we need to evaluate recursively, we need to search from the end of the string
      // otherwise, we only need to start searching from where the previous macro began
      int startPos = macroPosition.evaluateRecursively ? str.length() : macroPosition.startIndex - 1;
      macroPosition = findRightmostMacro(str, startPos);
    }
    return str;
  }

  /**
   * Find the rightmost macro in the specified string. If no macro is found, returns null.
   * The search will start at the specified position, then move left through the string.
   * Macros to the right of the specified position will be ignored.
   *
   * @param str the string to find a macro in
   * @param pos the position in the string to begin search for a macro.
   * @return the rightmost macro and its position in the string
   * @throws InvalidMacroException if invalid macro syntax was found.
   */
  @Nullable
  private MacroMetadata findRightmostMacro(String str, int pos) throws InvalidMacroException {
    // find opening "${" skipping escaped syntax "\${" and allowing doubly-escaping "\\${"
    int startIndex = getStartIndex(str, pos);
    if (startIndex < 0) {
      return null;
    }

    // found "${", now look for enclosing "}" and allow escaping "\}" and doubly-escaping "\\}"
    int endIndex = getFirstUnescapedTokenIndex('}', str, startIndex);
    if (endIndex < 0) {
      throw new InvalidMacroException(String.format("Could not find enclosing '}' for macro '%s'.",
                                                    str.substring(startIndex)));
    } else if (endIndex == startIndex + 2) {
      throw new InvalidMacroException("Macro expression cannot be empty");
    }

    // macroStr = 'macroFunction(macroArguments)' or just 'property' for ${property}
    String macroStr = str.substring(startIndex + 2, endIndex).trim();

    // look for "(", which indicates there are arguments and allow escaping "\(" and doubly-escaping "\\("
    int argsStartIndex = getFirstUnescapedTokenIndex('(', macroStr, 0);

    // determine whether to use a macro function or a property lookup
    if (argsStartIndex >= 0) {
      return getMacroFunctionMetadata(startIndex, endIndex, macroStr, argsStartIndex, str);
    } else {
      macroStr = replaceEscapedSyntax(macroStr);
      if (lookupsEnabled) {
        try {
          return new MacroMetadata(macroEvaluator.lookup(macroStr), startIndex, endIndex, true);
        } catch (InvalidMacroException e) {
          if (!skipInvalid) {
            throw e;
          }
        }
      }

      // if we are not evaluating lookups and are not skipping invalid macros,
      // return the original string with the surrounding ${},
      // and turn off recursive evaluation to prevent it from getting evaluated in an infinite loop
      return new MacroMetadata(String.format("${%s}", macroStr), startIndex, endIndex, false);
    }
  }

  /**
   * Use macro function to evaluate the macro string
   *
   * @param startIndex the start index of the macro string "$"
   * @param endIndex the end index of the macro string
   * @param macroStr the macro string to evaluate, without bracelet
   * @param argsStartIndex the index of start parenthesis
   * @param originalString the original string
   */
  private MacroMetadata getMacroFunctionMetadata(int startIndex, int endIndex, String macroStr, int argsStartIndex,
                                                 String originalString) {
    // check for closing ")" and allow escaping "\)" and doubly-escaping "\\)"
    int closingParenIndex = getFirstUnescapedTokenIndex(')', macroStr, 0);

    // this can only happen if we skip invalid or disable look up, the original string will contain unevaluated
    // macro, i.e {secure(${secure-key})}, in this case, the macroStr here will be "secure(${secure-key" and this
    // closing index will always be < 0.
    if ((!lookupsEnabled || skipInvalid) && closingParenIndex < 0) {
      // get ")" index using original string starting from end index
      int originalParenIndex = getFirstUnescapedTokenIndex(')', originalString, endIndex);
      // if found valid one, get the new macro string without "${" and set the correct closing ")" index
      if (originalParenIndex > endIndex) {
        // update end index to be next character after ")"
        endIndex = originalParenIndex + 1;
        // macro string should skip the first 2 characters "${"
        macroStr = originalString.substring(startIndex + 2, endIndex);
        closingParenIndex = getFirstUnescapedTokenIndex(')', macroStr, 0);
        // if this macro string contains unevaluated macros, there is no point to continue calling the macro function
        if (getStartIndex(macroStr, macroStr.length()) >= 0) {
          return new MacroMetadata(String.format("${%s}", macroStr), startIndex, endIndex, false);
        }
      }
    }

    if (closingParenIndex < 0 || !macroStr.endsWith(")")) {
      throw new InvalidMacroException(String.format("Could not find enclosing ')' for macro arguments in '%s'.",
                                                    macroStr));
    } else if (closingParenIndex != macroStr.length() - 1) {
      throw new InvalidMacroException(String.format("Macro arguments in '%s' have extra invalid trailing ')'.",
                                                    macroStr));
    }

    // arguments and macroFunction are expected to have escapes replaced when being evaluated
    String arguments = replaceEscapedSyntax(macroStr.substring(argsStartIndex + 1, macroStr.length() - 1));
    String macroFunction = replaceEscapedSyntax(macroStr.substring(0, argsStartIndex));

    String[] args = Iterables.toArray(Splitter.on(ARGUMENT_DELIMITER).split(arguments), String.class);
    // if function evaluation is disabled, or this is not a whitelisted function, don't perform any evaluation.
    // if the whitelist is empty, that means no whitelist was set, so every function should be evaluated.
    if (functionsEnabled && (functionWhitelist.isEmpty() || functionWhitelist.contains(macroFunction))) {
      try {
        switch (macroEvaluator.evaluateAs(macroFunction)) {
          case STRING:
            return new MacroMetadata(macroEvaluator.evaluate(macroFunction, args), startIndex, endIndex, true);
          case MAP:
            // evaluate the macro as map, and evaluate this map
            Map<String, String> properties = macroEvaluator.evaluateMap(macroFunction, args);
            Map<String, String> evaluated = new HashMap<>();
            properties.forEach((key, val) -> {
              evaluated.put(key, parse(val));
            });
            return new MacroMetadata(GSON.toJson(evaluated), startIndex, endIndex, true);
          default:
            // should not happen
            throw new IllegalStateException("Unsupported macro object type, the supported types are string and map.");
        }

      } catch (InvalidMacroException e) {
        if (!skipInvalid) {
          throw e;
        }
      }
    }
    // if we are not evaluating this function or are skipping invalid macros,
    // return the original string with the surrounding ${},
    // and turn off recursive evaluation to prevent it from getting evaluated in an infinite loop
    return new MacroMetadata(String.format("${%s}", macroStr), startIndex, endIndex, false);
  }

  /**
   * Get the start index of a macro in the string, starting from the specified position and searching left.
   * In other words, the index of the rightmost '${' that is still left of the specified position.
   *
   * @param str the string to search for the macro start syntax in
   * @param pos the position in the string to start the backwards search
   * @return the start index of the macro
   */
  private int getStartIndex(String str, int pos) {
    int startIndex = str.lastIndexOf("${", pos);
    while (isEscaped(startIndex, str)) {
      startIndex = str.substring(0, startIndex - 1).lastIndexOf("${");
    }
    return startIndex;
  }

  private int getFirstUnescapedTokenIndex(char token, String str, int startIndex) {
    int index = str.indexOf(token, startIndex);
    while (isEscaped(index, str)) {
      index = str.indexOf(token, index + 1);
    }
    return index;
  }

  /**
   * Returns whether or not the character at a given index in a string is escaped. Escaped
   * characters have an odd number of preceding backslashes.
   * @param index the index of the character to check for escaping
   * @param str the string in which the character is located at 'index'
   * @return if the character at the provided index is escaped
   */
  private boolean isEscaped(int index, String str) {
    if (!escapingEnabled) {
      return false;
    }
    int numPrecedingParens = 0;
    for (int i = index - 1; i >= 0; i--) {
      if (str.charAt(i) == '\\') {
        numPrecedingParens++;
      } else {
        break;
      }
    }
    // escaped tokens have an odd number of preceding backslashes
    return ((numPrecedingParens % 2) == 1);
  }

  /**
   * Strips all preceding backslash '\' characters.
   * @param str the string to replace escaped syntax in
   * @return the string with no escaped syntax
   */
  private String replaceEscapedSyntax(String str) {
    if (!escapingEnabled) {
      return str;
    }
    StringBuilder syntaxRebuilder = new StringBuilder();
    boolean includeNextConsecutiveBackslash = false;
    for (int i = 0; i < str.length(); i++) {
      if (str.charAt(i) != '\\' || includeNextConsecutiveBackslash) {
        syntaxRebuilder.append(str.charAt(i));
        includeNextConsecutiveBackslash = false;
      } else {
        includeNextConsecutiveBackslash = true;
      }
    }
    return syntaxRebuilder.toString();
  }

  private static final class MacroMetadata {
    private final String substitution;
    private final int startIndex;
    private final int endIndex;
    private final boolean evaluateRecursively;

    private MacroMetadata(String substitution, int startIndex, int endIndex, boolean evaluateRecursively) {
      this.substitution = substitution;
      this.startIndex = startIndex;
      this.endIndex = endIndex;
      this.evaluateRecursively = evaluateRecursively;
    }
  }

}
