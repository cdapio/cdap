/*
 * Copyright © 2017 Cask Data, Inc.
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

package co.cask.cdap.data.runtime.main;

import co.cask.cdap.common.app.MainClassLoader;

import java.net.URL;

/**
 * A classloader used for master service containers.
 */
public class MasterServiceMainClassLoader extends MainClassLoader {

  public MasterServiceMainClassLoader(URL[] urls, ClassLoader parent) {
    // Ignore the parent since it has HADOOP_CONF_DIR there, which can mess up explore container.
    // We have everything we needed setup via Twill.
    super(urls, null);
  }
}
