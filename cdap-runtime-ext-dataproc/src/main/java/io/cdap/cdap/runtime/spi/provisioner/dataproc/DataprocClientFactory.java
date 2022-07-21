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

import java.io.IOException;
import java.security.GeneralSecurityException;

/**
 * Creates DataprocClients. This exists primarily to allow cleanly mocking in tests.
 */
public interface DataprocClientFactory {

  /**
   * Create a {@link DataprocClient} for clusters that do not need SSH access.
   *
   * @param conf configuration about the dataproc clusters to operate on
   * @return a DataprocClient
   * @throws IOException if there was an exception reading the credentials
   * @throws GeneralSecurityException
   */
  default DataprocClient create(DataprocConf conf) throws GeneralSecurityException, IOException {
    return create(conf, false);
  }

  /**
   * Create a {@link DataprocClient} that can be used to perform dataproc operations.
   *
   * @param conf configuration about the dataproc clusters to operate on
   * @param requireSSH whether the cluster should be open to SSH connections. When false, the client can avoid
   *                   making various Compute calls to fetch additional information.
   * @return a DataprocClient
   * @throws IOException if there was an exception reading the credentials
   * @throws GeneralSecurityException
   */
  DataprocClient create(DataprocConf conf, boolean requireSSH) throws IOException, GeneralSecurityException;
}
