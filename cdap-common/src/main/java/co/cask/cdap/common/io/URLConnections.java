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

package co.cask.cdap.common.io;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.URLConnection;

/**
 * Utility class for {@link URLConnection}.
 */
public class URLConnections {
  private static final Logger LOG = LoggerFactory.getLogger(URLConnections.class);

  /**
   * Utility method to set {@link URLConnection#setDefaultUseCaches(boolean)}.
   */
  public static void setDefaultUseCaches(boolean useCaches) throws IOException {
    LOG.info("Turning off default caching in URLConnection");
    final String currentDir = System.getProperty("user.dir");
    new File(currentDir).toURI().toURL().openConnection().setDefaultUseCaches(useCaches);
  }
}
