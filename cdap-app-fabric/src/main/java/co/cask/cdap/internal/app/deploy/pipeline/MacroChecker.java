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
package co.cask.cdap.internal.app.deploy.pipeline;

import co.cask.cdap.internal.app.runtime.plugin.MacroParser;
import co.cask.cdap.internal.app.runtime.plugin.TrackingMacroEvaluator;

/**
 * to help check if a instance name (dataset, stream) is a macro
 */
public class MacroChecker {
  private final MacroParser parser;
  private final TrackingMacroEvaluator trackingMacroEvaluator;

  public MacroChecker() {
    this.trackingMacroEvaluator = new TrackingMacroEvaluator();
    this.parser = new MacroParser(trackingMacroEvaluator);
  }

  /**
   * check if instaneName is a macro and returns true if its a macro, false otherwise.
   * @param instanceName name of the instance
   * @return true if instanceName is a macro, false otherwise.
   */
  public boolean isMacro(String instanceName) {
    parser.parse(instanceName);
    if (trackingMacroEvaluator.hasMacro()) {
      trackingMacroEvaluator.reset();
      return true;
    }
    return false;
  }
}
