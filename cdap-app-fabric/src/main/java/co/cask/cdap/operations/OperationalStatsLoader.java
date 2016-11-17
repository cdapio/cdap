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

package co.cask.cdap.operations;

import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.extension.AbstractExtensionLoader;
import com.google.inject.Inject;

import java.util.Collections;
import java.util.ServiceLoader;
import java.util.Set;
import javax.management.MXBean;

/**
 * Class that registers {@link MXBean MXBeans} for reporting operational stats. To be loaded by this class, the
 * class that implements an {@link MXBean} should also additionally implement {@link OperationalStats}. This class loads
 * implementations of {@link OperationalStats} using the Java {@link ServiceLoader} architecture.
 */
public class OperationalStatsLoader extends AbstractExtensionLoader<OperationalExtensionId, OperationalStats> {

  @Inject
  OperationalStatsLoader(CConfiguration cConf) {
    super(cConf.get(Constants.OperationalStats.EXTENSIONS_DIR, ""));
  }

  @Override
  public Set<OperationalExtensionId> getSupportedTypesForProvider(OperationalStats operationalStats) {
    OperationalExtensionId operationalExtensionId = OperationalStatsUtils.getOperationalExtensionId(operationalStats);
    return operationalExtensionId == null ?
      Collections.<OperationalExtensionId>emptySet() :
      Collections.singleton(operationalExtensionId);
  }
}
