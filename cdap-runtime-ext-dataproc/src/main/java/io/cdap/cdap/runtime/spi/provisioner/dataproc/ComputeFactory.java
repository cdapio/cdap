/*
 *  Copyright © 2022 Cask Data, Inc.
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

package io.cdap.cdap.runtime.spi.provisioner.dataproc;

import com.google.api.services.compute.Compute;
import com.google.auth.oauth2.GoogleCredentials;

import java.io.IOException;
import java.security.GeneralSecurityException;

/**
 * Creates Compute instances.
 */
public interface ComputeFactory {

  /**
   * Create a Compute client.
   *
   * @param credentials credentials to use for the compute calls
   * @param connectTimeout connect timeout
   * @param readTimeout read timeout
   * @return Compute client
   * @throws GeneralSecurityException if there was a security issue creating the client
   * @throws IOException if there was an I/O exception creating the client
   */
  Compute createCompute(GoogleCredentials credentials, int connectTimeout,
                        int readTimeout) throws GeneralSecurityException, IOException;
}
