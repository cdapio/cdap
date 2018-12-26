/*
 * Copyright © 2018 Cask Data, Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License"); you may not
 *  use this file except in compliance with the License. You may obtain a copy of
 *  the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 *  WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 *  License for the specific language governing permissions and limitations under
 *  the License.
 */

package co.cask.cdap.securestore.spi;

import com.google.api.services.cloudkms.v1.CloudKMS;
import com.google.common.io.CharStreams;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;

/**
 * Configuration for {@link CloudKMS}.
 */
public class CloudKMSConf {
  // When created in the global location, Cloud KMS resources are available from zones spread around the world.
  private static final String LOCATION_ID = "global";
  // All the crypto keys are created under keyring named cdap
  private static final String KEYRING_ID = "cdap";
  private String projectId;

  /**
   * Constructs cloud kms conf.
   */
  public CloudKMSConf() {
    this.projectId = getSystemProjectId();
  }

  /**
   * @return project id
   */
  public String getProjectId() {
    return projectId;
  }

  /**
   * @return location id
   */
  public String getLocationId() {
    return LOCATION_ID;
  }

  /**
   * @return key ring id
   */
  public String getKeyringId() {
    return KEYRING_ID;
  }

  /**
   * Get project id from the metadata server. Makes a request to the metadata server that lives on the VM,
   * as described at https://cloud.google.com/compute/docs/storing-retrieving-metadata.
   */
  private static String getSystemProjectId() {
    try {
      URL url = new URL("http://metadata.google.internal/computeMetadata/v1/project/project-id");
      HttpURLConnection connection = null;
      try {
        connection = (HttpURLConnection) url.openConnection();
        connection.setRequestProperty("Metadata-Flavor", "Google");
        connection.connect();
        try (Reader reader = new InputStreamReader(connection.getInputStream(), StandardCharsets.UTF_8)) {
          return CharStreams.toString(reader);
        }
      } finally {
        if (connection != null) {
          connection.disconnect();
        }
      }
    } catch (IOException e) {
      throw new IllegalArgumentException("Unable to get project id from the environment. "
                                           + "Please explicitly set the project id and account key.", e);
    }
  }
}
