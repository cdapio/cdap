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

import java.io.File;
import java.io.FileNotFoundException;
import java.util.Map;

/**
 * Localization context for Program Tasks.
 */
@Beta
public interface TaskLocalizationContext {

  /**
   * Returns a {@link File} representing the path to a localized file localized with the given name.
   *
   * @param name the local file's name
   * @return a {@link File} for the localized file
   */
  File getLocalFile(String name) throws FileNotFoundException;

  /**
   * @return a map of file name to {@link File} for files localized for this program
   */
  Map<String, File> getAllLocalFiles();
}
