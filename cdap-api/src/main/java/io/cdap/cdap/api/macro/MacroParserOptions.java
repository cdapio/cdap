/*
 * Copyright © 2019 Cask Data, Inc.
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

package io.cdap.cdap.api.macro;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

/**
 * Options for macro parsing.
 */
public class MacroParserOptions {
  public static final MacroParserOptions DEFAULT = builder().build();
  private final boolean evaluateLookups;
  private final boolean evaluateFunctions;
  private final boolean escapingEnabled;
  private final boolean skipInvalid;
  private final int maxRecurseDepth;
  private final Set<String> functionWhitelist;

  private MacroParserOptions(boolean evaluateLookups, boolean evaluateFunctions,
                             boolean escapingEnabled, boolean skipInvalid,
                             int maxRecurseDepth, Set<String> functionWhitelist) {
    this.evaluateLookups = evaluateLookups;
    this.evaluateFunctions = evaluateFunctions;
    this.escapingEnabled = escapingEnabled;
    this.maxRecurseDepth = maxRecurseDepth;
    this.skipInvalid = skipInvalid;
    this.functionWhitelist = functionWhitelist;
  }

  public boolean shouldEvaluateLookups() {
    return evaluateLookups;
  }

  public boolean shouldEvaluateFunctions() {
    return evaluateFunctions;
  }

  public boolean shouldSkipInvalid() {
    return skipInvalid;
  }

  public boolean isEscapingEnabled() {
    return escapingEnabled;
  }

  public int getMaxRecurseDepth() {
    return maxRecurseDepth;
  }

  public Set<String> getFunctionWhitelist() {
    return functionWhitelist;
  }

  /**
   * @return Builder to create options
   */
  public static Builder builder() {
    return new Builder();
  }

  /**
   * Builds macro parser options.
   */
  public static class Builder {
    private boolean evaluateLookups = true;
    private boolean evaluateFunctions = true;
    private boolean escapingEnabled = true;
    private boolean skipInvalid = false;
    private int maxRecurseDepth = 10;
    private Set<String> functionWhitelist = new HashSet<>();

    public Builder disableLookups() {
      evaluateLookups = false;
      return this;
    }

    public Builder disableFunctions() {
      evaluateFunctions = false;
      return this;
    }

    public Builder skipInvalidMacros() {
      skipInvalid = true;
      return this;
    }

    public Builder setEscaping(boolean escapingEnabled) {
      this.escapingEnabled = escapingEnabled;
      return this;
    }

    public Builder setMaxRecurseDepth(int maxRecurseDepth) {
      this.maxRecurseDepth = maxRecurseDepth;
      return this;
    }

    public Builder setFunctionWhitelist(Collection<String> whitelist) {
      functionWhitelist.clear();
      functionWhitelist.addAll(whitelist);
      return this;
    }

    public Builder setFunctionWhitelist(String... whitelist) {
      return setFunctionWhitelist(Arrays.asList(whitelist));
    }

    public MacroParserOptions build() {
      return new MacroParserOptions(evaluateLookups, evaluateFunctions, escapingEnabled,
                                    skipInvalid, maxRecurseDepth, functionWhitelist);
    }
  }
}
