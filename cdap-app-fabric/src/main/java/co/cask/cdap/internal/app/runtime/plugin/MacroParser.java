/*
 * Copyright © 2016 Cask Data, Inc.
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

package co.cask.cdap.internal.app.runtime.plugin;

import co.cask.cdap.api.macro.InvalidMacroException;
import co.cask.cdap.api.macro.MacroEvaluator;
import com.google.common.base.Splitter;
import com.google.common.collect.Iterables;

import java.util.regex.Pattern;
import javax.annotation.Nullable;

/**
 * Provides a parser to validate the syntax of and evaluate macros.
 */
public class MacroParser {
  private static final String[] ESCAPED_TOKENS = {"\\${", "\\}", "\\(", "\\)"};
  private static final Pattern ARGUMENT_DELIMITER = Pattern.compile(",");
  private static final int MAX_SUBSTITUTION_DEPTH = 10;

  private final MacroEvaluator macroEvaluator;

  public MacroParser(MacroEvaluator macroEvaluator) {
    this.macroEvaluator = macroEvaluator;
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
      throw new InvalidMacroException(String.format("Failed substituting maco '%s', expansion exceeded %d levels.",
                                                    str, MAX_SUBSTITUTION_DEPTH));
    }

    MacroMetadata macroPosition = findRightmostMacro(str);
    while (macroPosition != null) {
      // in the case that the MacroEvaluator does not internally handle unspecified macros
      if (macroPosition.substitution == null) {
        throw new InvalidMacroException(String.format("Unable to substitute macro in string %s.", str));
      }
      str = str.substring(0, macroPosition.startIndex) +
            parse(macroPosition.substitution, depth + 1) +
            str.substring(macroPosition.endIndex + 1);
      macroPosition = findRightmostMacro(str);
    }
    return str;
  }

  /**
   * Find the rightmost macro in the specified string. If no macro is found, returns null.
   * @param str the string to find a macro in
   * @return the rightmost macro and its position in the string
   * @throws InvalidMacroException if invalid macro syntax was found.
   */
  @Nullable
  private MacroMetadata findRightmostMacro(String str)
    throws InvalidMacroException {
    // find opening "${" skipping escaped syntax "\${" TODO: and allowing doubly-escaping "\\${"
    int startIndex = getStartIndex(str);
    if (startIndex < 0) {
      return null;
    }

    // found "${", now look for enclosing "}" and allow escaping "\}" TODO: and doubly-escaping "\\}"
    int endIndex = getEndIndex(str, startIndex);
    if (endIndex < 0) {
      throw new InvalidMacroException(String.format("Could not find enclosing '}' for macro '%s'.",
                                                    str.substring(startIndex)));
    }

    // macroStr = 'macroFunction(macroArguments)' or just 'property' for ${property}
    String macroStr = str.substring(startIndex + 2, endIndex).trim();

    // look for "(", which indicates there are arguments and allow escaping "\(" TODO: and doubly-escaping "\\("
    int argsStartIndex = getArgsStartIndex(str, macroStr);

    // determine whether to use a macro function or a property lookup
    if (argsStartIndex >= 0) {
      return getMacroFunctionMetadata(startIndex, endIndex, macroStr, argsStartIndex);
    } else {
      macroStr = replaceEscapedSyntax(macroStr);
      return new MacroMetadata(macroEvaluator.lookup(macroStr), startIndex, endIndex);
    }
  }

  private MacroMetadata getMacroFunctionMetadata(int startIndex, int endIndex, String macroStr, int argsStartIndex) {
    // check for closing ")" and allow escaping "\)" TODO: and doubly-escaping "\\)"
    int closingParenIndex = getClosingParenIndex(macroStr);
    if (closingParenIndex < 0 || !macroStr.endsWith(")")) {
      throw new InvalidMacroException(String.format("Could not find enclosing ')' for macro arguments in '%s'.",
                                                    macroStr));
    }

    // arguments and macroFunction are expected to have escapes replaced when being evaluated
    String arguments = replaceEscapedSyntax(macroStr.substring(argsStartIndex + 1, macroStr.length() - 1));
    String macroFunction = replaceEscapedSyntax(macroStr.substring(0, argsStartIndex));

    String[] args = Iterables.toArray(Splitter.on(ARGUMENT_DELIMITER).split(arguments), String.class);
    return new MacroMetadata(macroEvaluator.evaluate(macroFunction, args), startIndex, endIndex);
  }

  private int getStartIndex(String str) {
    int startIndex = str.lastIndexOf("${");
    while (escaped(startIndex, str)) {
      // TODO: Break if doubly-escaped syntax
      startIndex = str.substring(0, startIndex - 1).lastIndexOf("${");
    }
    return startIndex;
  }

  private int getEndIndex(String str, int startIndex) {
    int endIndex = str.indexOf('}', startIndex);
    while (escaped(endIndex, str)) {
      // TODO: Break if doubly-escaped syntax
      endIndex = str.indexOf('}', endIndex + 1);
    }
    return endIndex;
  }

  private int getArgsStartIndex(String str, String macroStr) {
    int argsStartIndex = macroStr.indexOf('(');
    while (escaped(argsStartIndex, str)) {
      // TODO: Break if doubly-escaped syntax
      argsStartIndex = macroStr.indexOf('(', argsStartIndex);
    }
    return argsStartIndex;
  }

  private int getClosingParenIndex(String macroStr) {
    int closingParenIndex = macroStr.lastIndexOf(')');
    while (escaped(closingParenIndex, macroStr)) {
      // TODO: Break if doubly-escaped syntax
      closingParenIndex = macroStr.substring(0, closingParenIndex).lastIndexOf(')');
    }
    return closingParenIndex;
  }

  private boolean escaped(int index, String str) {
    return (index > 0 && str.charAt(index - 1) == '\\');
  }

  private boolean doublyEscaped(int index, String str) {
    return (index > 1 && str.charAt(index - 2) == '\\');
  }

  /**
   * Removes all escaped syntax for all macro syntax symbols: ${, }, (, )
   * TODO: Handle doubly-escaped syntax (e.g. \\${macro} should still be evaluated)
   * @param str the string to replace escaped syntax in
   * @return the string with no escaped syntax
   */
  private String replaceEscapedSyntax(String str) {
    for (String token : ESCAPED_TOKENS) {
      str = str.replace(token, token.substring(1));
    }
    return str;
  }

  private static final class MacroMetadata {
    private final String substitution;
    private final int startIndex;
    private final int endIndex;

    private MacroMetadata(String substitution, int startIndex, int endIndex) {
      this.substitution = substitution;
      this.startIndex = startIndex;
      this.endIndex = endIndex;
    }
  }
}
