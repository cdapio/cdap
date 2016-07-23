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

import co.cask.cdap.api.macro.MacroFunction;
import com.google.common.collect.ImmutableList;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 *
 */
public class CollectMacroEvaluatorTest {

  @Test
  public void testMacrosCollected() throws Exception {
    CollectMacroEvaluator collectMacroEvaluator = new CollectMacroEvaluator();
    MacroParser parser = new MacroParser(collectMacroEvaluator);
    List<String> macros = new ArrayList<>();
    macros.add("${hello}${world}");
    macros.add("${secure2(password)}");
    macros.add("${hello}");
    for (String macro : macros) {
      parser.parse(macro);
    }

    Set<String> expectedLookups = new HashSet<>();
    expectedLookups.add("hello");
    expectedLookups.add("world");

    Assert.assertEquals(expectedLookups, collectMacroEvaluator.getMacros().getLookups());


    Map<String, List<String>> functionNameToArguments = new HashMap<>();
    functionNameToArguments.put("secure2", ImmutableList.of("password"));

    for (MacroFunction macroFunction : collectMacroEvaluator.getMacros().getMacroFunctions()) {
      Assert.assertTrue(functionNameToArguments.containsKey(macroFunction.getFunctionName()));
      Assert.assertEquals(functionNameToArguments.get(macroFunction.getFunctionName()),
                          macroFunction.getArguments());
    }
  }
}
