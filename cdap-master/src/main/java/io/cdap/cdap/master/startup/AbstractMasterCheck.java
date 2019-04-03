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

package co.cask.cdap.master.startup;

import co.cask.cdap.common.MasterUtils;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.startup.Check;

import java.util.Set;

/**
 * Base for master startup checks.
 */
public abstract class AbstractMasterCheck extends Check {
  protected final CConfiguration cConf;
  protected final Set<ServiceResourceKeys> systemServicesResourceKeys;

  protected AbstractMasterCheck(CConfiguration cConf) {
    this.cConf = cConf;
    this.systemServicesResourceKeys = MasterUtils.createSystemServicesResourceKeysSet(cConf);
  }
}
