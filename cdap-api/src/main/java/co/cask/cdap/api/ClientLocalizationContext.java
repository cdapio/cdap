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

package co.cask.cdap.api;

import co.cask.cdap.api.annotation.Beta;

import java.net.URI;

/**
 * Context that adds the capability to localize resources in programs.
 */
@Beta
public interface ClientLocalizationContext {

  /**
   * Localizes a file with the given name. The localized file can be retrieved at program runtime using
   * {@link TaskLocalizationContext#getLocalFile(String)}.
   *
   * @param name the name for the localized file. The localized file can be retrieved using this name at program
   *             runtime using {@link TaskLocalizationContext#getLocalFile(String)}
   * @param uri the {@link URI} of the file to localize
   */
  void localize(String name, URI uri);

  /**
   * Localizes a file with the given name. The localized file can be retrieved at program runtime using
   * {@link TaskLocalizationContext#getLocalFile(String)}.
   *
   * @param name the name for the localized file. The localized file can be retrieved using this name at program
   *             runtime using {@link TaskLocalizationContext#getLocalFile(String)}
   * @param uri the {@link URI} of the file to localize
   * @param archive indicates if the file is an archive
   */
  void localize(String name, URI uri, boolean archive);
}
