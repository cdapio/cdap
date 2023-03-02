/*
 * Copyright © 2022 Cask Data, Inc.
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

package io.cdap.cdap.internal.app.program;

import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import java.util.Map;
import java.util.Set;
import org.apache.twill.api.TwillPreparer;

/**
 * Utility class for launching twill runnables.
 */
public class LauncherUtils {

  /**
   * Sets the JVM options overrides for all twill runnables with an override config set.
   *
   * @param cConf The configuration to use
   * @param twillPreparer The twill preparer to use
   * @param runnables The set of runnables to override
   */
  public static void overrideJVMOpts(CConfiguration cConf, TwillPreparer twillPreparer,
      Set<String> runnables) {
    Map<String, String> jvmOverrideConfig = cConf.getPropsWithPrefix(
        Constants.AppFabric.PROGRAM_JVM_OPTS_PREFIX);
    for (Map.Entry<String, String> jvmOverrideEntry : jvmOverrideConfig.entrySet()) {
      String jvmOverrideKey = jvmOverrideEntry.getKey();
      if (runnables.contains(jvmOverrideKey)) {
        twillPreparer.setJVMOptions(jvmOverrideKey, jvmOverrideEntry.getValue());
      }
    }
  }
}
