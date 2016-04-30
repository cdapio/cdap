/*
 * Copyright © 2015 Cask Data, Inc.
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

package co.cask.cdap.internal.app.runtime;

import co.cask.cdap.api.TaskLocalizationContext;
import com.google.common.collect.ImmutableMap;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.Serializable;
import java.util.Map;

/**
 * An implementation of {@link TaskLocalizationContext} that can be initialized with some localized resources.
 */
public class DefaultTaskLocalizationContext implements TaskLocalizationContext, Serializable {

  private final Map<String, File> localizedResources;

  public DefaultTaskLocalizationContext(Map<String, File> localizedResources) {
    this.localizedResources = ImmutableMap.copyOf(localizedResources);
  }

  @Override
  public File getLocalFile(String name) throws FileNotFoundException {
    if (!localizedResources.containsKey(name)) {
      throw new FileNotFoundException(String.format("The specified file %s was not found. Please make sure it was " +
                                                      "localized using context.localize().", name));
    }
    return localizedResources.get(name);
  }

  @Override
  public Map<String, File> getAllLocalFiles() {
    return localizedResources;
  }
}
