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

package co.cask.cdap.client.util;

import co.cask.cdap.client.config.ClientConfig;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.proto.ProgramType;
import com.google.common.base.Preconditions;

import java.net.MalformedURLException;
import java.net.URL;

/**
 * Utility methods used by CDAP Clients for CDAP API version migration
 */
public class VersionMigrationUtils {

  private VersionMigrationUtils() {
  }

  /**
   * @param config the config object being used by a client
   * @return true if procedures are supported with the given configuration; false otherwise.
   */
  public static boolean isProcedureSupported(ClientConfig config) {
    return Constants.DEFAULT_NAMESPACE.equals(config.getNamespace());
  }

  /**
   * @param config the config object being used by a client
   * @param programType the type of program being checked
   * @return true if the programType is supported by the given configuration; false otherwise.
   */
  public static boolean isProgramSupported(ClientConfig config, ProgramType programType) {
    return !(ProgramType.PROCEDURE == programType) || isProcedureSupported(config);
  }

  /**
   * @throws IllegalStateException if procedures are not supported with the given configuration
   * @param config the config object being used by a client
   */
  public static void assertProcedureSupported(ClientConfig config) {
    Preconditions.checkState(isProcedureSupported(config),
                             "Procedure operations are only supported in the default namespace.");
    Preconditions.checkState(Constants.Gateway.API_VERSION_2_TOKEN.equals(config.getApiVersion()),
                             "Procedure operations are only supported in V2 APIs");
  }

  /**
   * Uses v2 or v3 APIs depending on the program type, due to the fact that procedures are not supported in v3 APIs.
   * @param config the config object being used by a client
   * @param programType type of program on which an operation is being executed
   * @param path endpoint attempted to hit
   * @return resolved URL for the specified path
   * @throws MalformedURLException
   */
  public static URL resolveURL(ClientConfig config, ProgramType programType, String path) throws MalformedURLException {
    if (ProgramType.PROCEDURE == programType) {
      VersionMigrationUtils.assertProcedureSupported(config);
      return config.resolveURL(path);
    } else {
      return config.resolveNamespacedURLV3(path);
    }
  }
}
