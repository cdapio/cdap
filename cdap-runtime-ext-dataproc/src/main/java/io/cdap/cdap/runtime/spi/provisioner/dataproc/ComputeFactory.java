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
import java.io.IOException;
import java.security.GeneralSecurityException;

/**
 * Creates Compute instances. This exists primarily to allow cleanly mocking in tests.
 */
public interface ComputeFactory {

  /**
   * Create a Compute client.
   *
   * @param conf configuration for dataproc calls, containing things like the connect and read
   *     timeouts
   * @return Compute client
   * @throws GeneralSecurityException if there was a security issue creating the client
   * @throws IOException if there was an I/O exception creating the client
   */
  Compute createCompute(DataprocConf conf) throws GeneralSecurityException, IOException;
}
