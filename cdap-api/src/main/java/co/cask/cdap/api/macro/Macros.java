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

package co.cask.cdap.api.macro;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;

/**
 * Class encapsulating lookup property macros and macro functions
 */
public class Macros implements Serializable {
  private static final long serialVersionUID = 1606313949471664886L;
  private final Set<String> lookupProperties;
  private final Set<MacroFunction> macroFunctions;

  public Macros(Set<String> lookupProperties, Set<MacroFunction> macroFunctions) {
    this.lookupProperties = lookupProperties;
    this.macroFunctions = macroFunctions;
  }

  public Macros() {
    this.lookupProperties = new HashSet<>();
    this.macroFunctions = new HashSet<>();
  }

  public Set<String> getLookups() {
    return lookupProperties;
  }

  public Set<MacroFunction> getMacroFunctions() {
    return macroFunctions;
  }
}
