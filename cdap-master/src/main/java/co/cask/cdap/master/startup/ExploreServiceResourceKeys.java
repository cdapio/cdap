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

import co.cask.cdap.common.conf.CConfiguration;

/**
 * Extends the ServiceResourceKeys class to override the number of instances for the explore service.
 */
public class ExploreServiceResourceKeys extends ServiceResourceKeys {

  public ExploreServiceResourceKeys(CConfiguration cConf, String serviceName, String memoryKey, String vcoresKey) {
    super(cConf, serviceName, memoryKey, vcoresKey, null, null);
  }

  @Override
  public int getInstances() {
    return 1;
  }

  @Override
  public int getMaxInstances() {
    return 1;
  }

}
