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

package io.cdap.cdap.internal.app.runtime.plugin;


import com.google.common.collect.ImmutableMap;
import io.cdap.cdap.api.macro.InvalidMacroException;
import io.cdap.cdap.api.macro.MacroEvaluator;
import io.cdap.cdap.api.macro.MacroParserOptions;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class MacroParserTest {

  // Test containsMacro Parsing

  @Test
  public void testContainsUndefinedMacro() throws InvalidMacroException {
    assertContainsMacroParsing("${undefined}", true);
  }

  @Test
  public void testContainsExcessivelyEscapedMacro() throws InvalidMacroException {
    assertContainsMacroParsing("\\${${macro}}\\${${{\\}}}\\escaped\\${one}${fun${\\${escapedMacroLiteral\\}}}\\no-op",
                               true);
  }

  @Test
  public void testContainsSimpleEscapedMacro() throws InvalidMacroException {
    assertContainsMacroParsing("$${{\\}}", true);
    assertSubstitution("\\${val}", "${val}", Collections.emptyMap(), Collections.emptyMap());
  }

  @Test
  public void testContainsDoublyEscapedMacro() throws InvalidMacroException {
    assertContainsMacroParsing("\\\\file\\\\path\\\\name\\\\${filePathMacro}", true);
  }

  @Test
  public void testContainsConsecutiveMacros() throws InvalidMacroException {
    assertContainsMacroParsing("${simpleHostname}/${simplePath}:${simplePort}", true);
  }

  @Test
  public void testContainsManyMacros() throws InvalidMacroException {
    assertContainsMacroParsing("${l}${o}${c}${a}${l}${hostSuffix}", true);
  }

  @Test
  public void testContainsNestedMacros() throws InvalidMacroException {
    assertContainsMacroParsing("${nested${macros${are${ok}}}}", true);
  }

  @Test(expected = InvalidMacroException.class)
  public void testContainsBadlyFormattedNestedMacros() {
    assertContainsMacroParsing("${nested${macros${are${ok}}}", true);
  }

  @Test(expected = InvalidMacroException.class)
  public void containsBadlyFormattedMacro() throws InvalidMacroException {
    assertContainsMacroParsing("${badFormatting", false);
  }

  @Test
  public void containsEmptyMacro() throws InvalidMacroException {
    assertContainsMacroParsing("${}", true);
  }

  @Test
  public void containsNoMacro() throws InvalidMacroException {
    assertContainsMacroParsing("hostname/path:port", false);
  }

  @Test
  public void containsNoEscapedMacro() {
    assertContainsMacroParsing("\\{definitely\\{not\\{test(aMacro)}}}", false);
  }


  // Test Macro Substitution

  @Test(expected = InvalidMacroException.class)
  public void testUndefinedMacro() throws InvalidMacroException {
    assertSubstitution("${undefined}", "", Collections.emptyMap(), Collections.emptyMap());
  }

  /**
   * Tests if the empty string can be passed macro-substituted.
   * Expands the following macro tree of depth 1:
   *
   *             ${}
   *              |
   *           emptyMacro
   */
  @Test
  public void testEmptyMacro() throws InvalidMacroException {
    Map<String, String> properties = ImmutableMap.<String, String>builder()
      .put("", "emptyMacro")
      .build();
    assertSubstitution("${}", "emptyMacro", properties, Collections.emptyMap());
  }

  /**
   * Tests if empty string can be passed as arguments to a macro function.
   * Expands the following macro tree of depth 1:
   *
   *          ${test()}
   *              |
   *          emptyMacro
   */
  @Test
  public void testEmptyArguments() throws InvalidMacroException {
    Map<String, String> macroFunctionSubstitutions = ImmutableMap.<String, String>builder()
      .put("", "emptyMacro")
      .build();
    assertSubstitution("${test()}", "emptyMacro", Collections.emptyMap(), macroFunctionSubstitutions);
  }

  /**
   * Tests if escape indicator '\' is not replaced when not followed by a macro syntax token.
   * Expands the following macro tree of depth 2:
   *
   *     ------ \test\${${macro}}\${${}}\escaped\${one}${fun${\${escapedMacroLiteral\}}}\no-op ------
   *     |                                        |                                                 |
   * \test${42}                       ${brackets}\escaped${one}                             ${funTimes}\no-op
   *                                                                                                |
   *                                                                                            ahead\no-op
   */
  @Test
  public void testNoUnnecessaryReplacement() {
    Map<String, String> properties = ImmutableMap.<String, String>builder()
      .put("{}", "brackets")
      .put("macro", "42")
      .put("${escapedMacroLiteral}", "Times")
      .put("escapedMacroLiteral", "SHOULD NOT EVALUATE")
      .put("funTimes", "ahead")
      .build();
    assertSubstitution("\\\\test\\${${macro}}\\${${{\\}}}-escaped\\${one}${fun${\\${escapedMacroLiteral\\}}}\\\\no-op",
                       "\\test${42}${brackets}-escaped${one}ahead\\no-op", properties,
                       Collections.emptyMap());
  }

  /**
   * Tests simple escaping of property macros.
   * Expands the following macro tree of depth 1:
   *
   *              $${{\}}
   *                 |
   *             $brackets
   */
  @Test
  public void propertyBracketEscapingTest() throws InvalidMacroException {
    Map<String, String> properties = ImmutableMap.<String, String>builder()
      .put("{}", "brackets")
      .build();
    assertSubstitution("$${{\\}}", "$brackets", properties, Collections.emptyMap());
  }

  @Test
  public void testRidiculousSyntaxEscaping() throws InvalidMacroException {
    Map<String, String> properties = ImmutableMap.<String, String>builder()
      .put("{}", "brackets")
      .put("macro", "42")
      .put("${escapedMacroLiteral}", "Times")
      .put("escapedMacroLiteral", "SHOULD NOT EVALUATE")
      .put("funTimes", "ahead")
      .build();
    assertSubstitution("\\${${macro}}\\${${{\\}}}\\${one}${fun${\\${escapedMacroLiteral\\}}}",
                       "${42}${brackets}${one}ahead", properties, Collections.emptyMap());
  }

  /**
   * Tests if an exception is thrown on a nonexistent/unspecified macro.
   */
  @Test(expected = InvalidMacroException.class)
  public void testNonexistentMacro() throws InvalidMacroException {
    assertSubstitution("${test(invalid)}", "", Collections.emptyMap(), Collections.emptyMap());
  }

  /**
   * Tests if an exception is thrown if substitution depth exceeds a maximum with a circular key.
   * Expands the following macro tree of depth infinity:
   *
   *                      ${key} ---
   *                        |      |
   *                      ${key} ---
   */
  @Test(expected = InvalidMacroException.class)
  public void testCircularMacro() throws InvalidMacroException {
    Map<String, String> macroFunctionSubstitutions = ImmutableMap.<String, String>builder()
      .put("key", "${test(key)}")
      .build();
    assertSubstitution("${test(key)}", "", Collections.emptyMap(), macroFunctionSubstitutions);
  }

  /**
   * Tests simple macro syntax escaping.
   */
  @Test
  public void testSimpleMacroSyntaxEscaping() throws InvalidMacroException {
    Map<String, String> macroFunctionSubstitutions = ImmutableMap.<String, String>builder()
      .put("simpleEscape", "\\${test(\\${test(expansiveHostnameTree)})}")
      .build();
    assertSubstitution("${test(simpleEscape)}", "${test(${test(expansiveHostnameTree)})}",
                       Collections.emptyMap(), macroFunctionSubstitutions);
  }

  /**
   * Tests advanced macro syntax escaping
   * Expands the following macro tree of depth 2:
   *
   *                                ${test(advancedEscape)}
   *                                          |
   *                               ${test(lotsOfEscaping)}
   *                                          |
   *      ${test(simpleHostnameTree)${test(first)}${test(filename${test(fileTypeMacro))}
   */
  @Test
  public void testAdvancedMacroSyntaxEscaping() throws InvalidMacroException {
    Map<String, String> macroFunctionSubstiutions = ImmutableMap.<String, String>builder()
      .put("advancedEscape", "${test(lotsOfEscaping)}")
      .put("lotsOfEscaping", "\\${test(simpleHostnameTree)\\${test(first)}\\${test(filename\\${test(fileTypeMacro))}")
      .build();
    assertSubstitution("${test(advancedEscape)}",
                       "${test(simpleHostnameTree)${test(first)}${test(filename${test(fileTypeMacro))}",
                       Collections.emptyMap(), macroFunctionSubstiutions);
  }


  /**
   * Tests expansive use of macro syntax escaping.
   * Expands the following macro tree of depth 3:
   *
   *      ${test(${test(\${test(macroLiteral)\})\})}\${test(nothing)}${test(simplePath} -----------------
   *                                               |                                                    |
   *                                         ${test(match)})}                               ${test(nothing)}index.html
   *                                               |
   *           --- {test(dontEvaluate):${test(firstPortDigit)}0\${test-\${test(null)}\${\${\${nil ---
   *           |                                      |                                             |
   *   {test(dontEvaluate):                           8                                0${test-${test(null)}${${${nil
   *
   */
  @Test
  public void testExpansiveSyntaxEscaping() throws InvalidMacroException {
    Map<String, String> macroFunctionSubstiutions = ImmutableMap.<String, String>builder()
      .put("expansiveEscape", "${test(${test(\\${test(macroLiteral\\)\\})})}\\${test(nothing)${test(simplePath)}")
      .put("${test(macroLiteral)}", "match")
      .put("match", "{test(dontEvaluate):${test(firstPortDigit)}0\\${test-\\${test(null)}\\${\\${\\${nil")
      .put("simplePath", "index.html")
      .put("firstPortDigit", "8")
      .put("secondPortDigit", "0")
      .build();
    assertSubstitution("${test(expansiveEscape)}",
                       "{test(dontEvaluate):80${test-${test(null)}${${${nil${test(nothing)index.html",
                       Collections.emptyMap(), macroFunctionSubstiutions);
  }

  // Double escaping tests

  @Test
  public void testSimpleDoubleEscaping() throws InvalidMacroException {
    Map<String, String> properties = ImmutableMap.<String, String>builder()
      .put("filePathMacro", "executable.exe")
      .build();
    assertSubstitution("\\\\file\\\\path\\\\name\\\\${filePathMacro}", "\\file\\path\\name\\executable.exe", properties,
                       Collections.emptyMap());
  }

  /**
   * Tests for arguments that end with '\' before the closing ')'.
   */
  @Test
  public void testSimpleMacroFunctionDoubleEscaping() throws InvalidMacroException {
    Map<String, String> macroFunctionSubstitutions = ImmutableMap.<String, String>builder()
      .put("test\\", "password")
      .build();
    assertSubstitution("${test(test\\\\)}", "password", Collections.emptyMap(), macroFunctionSubstitutions);
  }

  /**
   * Tests for detection of trailing ')' in macro string.
   */
  @Test(expected = InvalidMacroException.class)
  public void testInvalidFunctionSyntaxEscaping() throws InvalidMacroException {
    Map<String, String> macroFunctionSubstitutions = ImmutableMap.<String, String>builder()
      .put("test\\)", "password")
      .build();
    assertSubstitution("${test(test\\\\))}", "password", Collections.emptyMap(), macroFunctionSubstitutions);
  }


  /**
   * Tests for escaping of both backslash and parenthesis
   */
  @Test
  public void testSimpleMacroFunctionEscapeAndDoubleEscape() throws InvalidMacroException {
    Map<String, String> macroFunctionSubstitutions = ImmutableMap.<String, String>builder()
      .put("test\\)", "password")
      .build();
    assertSubstitution("${test(test\\\\\\))}", "password", Collections.emptyMap(), macroFunctionSubstitutions);
  }

  /**
   * Tests for properties that end with '\' before the closing '}'.
   */
  @Test
  public void testSimplePropertyDoubleEscaping() throws InvalidMacroException {
    Map<String, String> properties = ImmutableMap.<String, String>builder()
      .put("test\\}", "password")
      .build();
    assertSubstitution("${test\\\\\\}}", "password", properties, Collections.emptyMap());
  }

  /**
   * Tests for escaping of both backslash and closing '}'.
   */
  @Test
  public void testSimplePropertyEscapeAndDoublEscape() throws InvalidMacroException {
    Map<String, String> properties = ImmutableMap.<String, String>builder()
      .put("test\\}", "password")
      .build();
    assertSubstitution("${test\\\\\\}}", "password", properties, Collections.emptyMap());
  }


  // Other macro tree tests

  /**
   * Tests a simple property tree expansion.
   * Expands the following macro tree of depth 2:
   *
   *                                ${simpleHostnameTree}
   *                                          |
   *           -------- ${simpleHostname}/${simplePath}:${simplePort} --------
   *           |                              |                              |
   *        localhost                     index.html                        80
   */
  @Test
  public void testSimplePropertyTree() {
    Map<String, String> properties = ImmutableMap.<String, String>builder()
      .put("simpleHostnameTree", "${simpleHostname}/${simplePath}:${simplePort}")
      .put("simpleHostname", "localhost")
      .put("simplePath", "index.html")
      .put("simplePort", "80")
      .build();
    assertSubstitution("${simpleHostnameTree}", "localhost/index.html:80", properties, new HashMap<>());
  }

  /**
   * Tests an advanced property tree expansion.
   * Expands the following macro tree of depth 4:
   *
   *                      ${advancedHostnameTree}
   *                                |
   *              --------- ${first}/${second} ---------------------
   *              |                                                |
   *          localhost                             ------ ${third}:${sixth} -----------
   *                                                |                                   |
   *                                ------- ${fourth}${fifth} -------                  80
   *                                |                               |
   *                              index                           .html
   */
  @Test
  public void testAdvancedPropertyTree() throws InvalidMacroException {
    Map<String, String> properties = ImmutableMap.<String, String>builder()
      .put("advancedHostnameTree", "${first}/${second}")
      .put("first", "localhost")
      .put("second", "${third}:${sixth}")
      .put("third", "${fourth}${fifth}")
      .put("fourth", "index")
      .put("fifth", ".html")
      .put("sixth", "80")
      .build();
    assertSubstitution("${advancedHostnameTree}", "localhost/index.html:80", properties, Collections.emptyMap());
  }

  /**
   * Tests an expansive property tree expansion.
   * Expands the following macro tree of depth 6:
   *
   *                              ${expansiveHostnameTree}
   *                                        |
   *                  ---------- ${hostname}/${path}:${port} -------------------
   *                 |                      |                                  |
   *               ${one}                 ${two}                        --- ${three} -------
   *                 |                      |                           |                  |
   *        ${host${hostScopeMacro}} ${filename${fileTypeMacro}} ${firstPortDigit} ${secondPortDigit}
   *                 |                      |                           |                  |
   *           ${host-local}          ${filename-html}                  8                  0
   *                 |                      |
   *  ${l}${o}${c}${a}${l}${hostSuffix}  index.html
   *    |   |   |   |   |       |
   *    l   o   c   a   l      host
   *
   */
  @Test
  public void testExpansivePropertyTree() throws InvalidMacroException {
    Map<String, String> properties = ImmutableMap.<String, String>builder()
      .put("expansiveHostnameTree", "${hostname}/${path}:${port}")
      .put("hostname", "${one}")
      .put("path", "${two}")
      .put("port", "${three}")
      .put("one", "${host${hostScopeMacro}}")
      .put("hostScopeMacro", "-local")
      .put("host-local", "${l}${o}${c}${a}${l}${hostSuffix}")
      .put("l", "l")
      .put("o", "o")
      .put("c", "c")
      .put("a", "a")
      .put("hostSuffix", "host")
      .put("two", "${filename${fileTypeMacro}}")
      .put("three", "${firstPortDigit}${secondPortDigit}")
      .put("filename", "index")
      .put("fileTypeMacro", "-html")
      .put("filename-html", "index.html")
      .put("filename-php", "index.php")
      .put("firstPortDigit", "8")
      .put("secondPortDigit", "0")
      .build();
    assertSubstitution("${expansiveHostnameTree}", "localhost/index.html:80", properties, Collections.emptyMap());
  }

  /**
   * See simple, advanced, and expansive tree tests for details.
   */
  @Test
  public void stressTestPropertyTree() throws InvalidMacroException {
    Map<String, String> properties = ImmutableMap.<String, String>builder()
      // simple hostname tree
      .put("simpleHostnameTree", "${simpleHostname}/${simplePath}:${simplePort}")
      .put("simpleHostname", "localhost")
      .put("simplePath", "index.html")
      .put("simplePort", "80")
      // advanced hostname tree
      .put("advancedHostnameTree", "${first}/${second}")
      .put("first", "localhost")
      .put("second", "${third}:${sixth}")
      .put("third", "${fourth}${fifth}")
      .put("fourth", "index")
      .put("fifth", ".html")
      .put("sixth", "80")
      // expansive hostname tree
      .put("expansiveHostnameTree", "${hostname}/${path}:${port}")
      .put("hostname", "${one}")
      .put("path", "${two}")
      .put("port", "${three}")
      .put("one", "${host${hostScopeMacro}}")
      .put("hostScopeMacro", "-local")
      .put("host-local", "${l}${o}${c}${a}${l}${hostSuffix}")
      .put("l", "l")
      .put("o", "o")
      .put("c", "c")
      .put("a", "a")
      .put("hostSuffix", "host")
      .put("two", "${filename${fileTypeMacro}}")
      .put("three", "${firstPortDigit}${secondPortDigit}")
      .put("filename", "index")
      .put("fileTypeMacro", "-html")
      .put("filename-html", "index.html")
      .put("filename-php", "index.php")
      .put("firstPortDigit", "8")
      .put("secondPortDigit", "0")
      .build();
    assertSubstitution("${simpleHostnameTree}${simpleHostnameTree}${simpleHostnameTree}" +
                       "${advancedHostnameTree}${advancedHostnameTree}${advancedHostnameTree}" +
                       "${expansiveHostnameTree}${expansiveHostnameTree}${expansiveHostnameTree}" +
                       "${simpleHostnameTree}${advancedHostnameTree}${expansiveHostnameTree}",
                       "localhost/index.html:80localhost/index.html:80localhost/index.html:80localhost/index.html:80" +
                       "localhost/index.html:80localhost/index.html:80localhost/index.html:80localhost/index.html:80" +
                       "localhost/index.html:80localhost/index.html:80localhost/index.html:80localhost/index.html:80",
                       properties, new HashMap<>());
  }

  @Test
  public void testNoEscape() {
    MacroEvaluator evaluator = new TestMacroEvaluator(ImmutableMap.of("\\a\\b\\c\\", "123", "x", "xyz"),
                                                      ImmutableMap.of("xyz", "321"));
    MacroParser parser = new MacroParser(evaluator, MacroParserOptions.builder().setEscaping(false).build());
    Assert.assertEquals("123", parser.parse("${\\a\\b\\c\\}"));
    Assert.assertEquals("321", parser.parse("${test(${x})}"));
    Assert.assertEquals("\\321", parser.parse("\\${test(xyz)}"));
  }

  @Test
  public void testDisableLookups() {
    MacroEvaluator evaluator = new TestMacroEvaluator(Collections.emptyMap(), Collections.emptyMap());
    MacroParser parser = new MacroParser(evaluator, MacroParserOptions.builder().disableLookups().build());
    Assert.assertEquals("${key}", parser.parse("${key}"));
  }

  @Test
  public void testDisableFunctions() {
    MacroEvaluator evaluator = new TestMacroEvaluator(Collections.emptyMap(), Collections.emptyMap());
    MacroParser parser = new MacroParser(evaluator, MacroParserOptions.builder().disableFunctions().build());
    Assert.assertEquals("${test(key)}", parser.parse("${test(key)}"));
  }

  @Test
  public void testFunctionWhitelist() {
    MacroEvaluator evaluator = new TestMacroEvaluator(Collections.emptyMap(), ImmutableMap.of("key", "val"));
    MacroParser parser = new MacroParser(evaluator, MacroParserOptions.builder().setFunctionWhitelist("t").build());
    // $t(key) should get evaluated, but $test(key) should get skipped.
    Assert.assertEquals("${test(key)}val", parser.parse("${test(key)}${t(key)}"));
    Assert.assertEquals("val${test(key)}", parser.parse("${t(key)}${test(key)}"));
    Assert.assertEquals("${test(val)}", parser.parse("${test(${t(key)})}"));
  }

  @Test
  public void testSkipInvalidMacros() {
    MacroEvaluator evaluator = new TestMacroEvaluator(Collections.emptyMap(), Collections.emptyMap());
    MacroParser parser = new MacroParser(evaluator, MacroParserOptions.builder().skipInvalidMacros().build());
    Assert.assertEquals("${k1}", parser.parse("${k1}"));
    Assert.assertEquals("${test(key)}", parser.parse("${test(key)}"));
    Assert.assertEquals("abc${123}", parser.parse("abc${123}"));
  }

  // Testing util methods

  private static void assertContainsMacroParsing(String macro, boolean expected) {
    TrackingMacroEvaluator trackingMacroEvaluator = new TrackingMacroEvaluator();
    new MacroParser(trackingMacroEvaluator).parse(macro);
    Assert.assertEquals(trackingMacroEvaluator.hasMacro(), expected);
  }

  private static void assertSubstitution(String macro, String expected, Map<String, String> propertySubstitutions,
                                         Map<String, String> macroFunctionSubstitutions) {
    MacroEvaluator macroEvaluator = new TestMacroEvaluator(propertySubstitutions, macroFunctionSubstitutions);
    MacroParser macroParser = new MacroParser(macroEvaluator);
    Assert.assertEquals(expected, macroParser.parse(macro));
  }
}
