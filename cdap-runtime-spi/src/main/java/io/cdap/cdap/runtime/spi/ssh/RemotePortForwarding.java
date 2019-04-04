/*
 * Copyright © 2019 Cask Data, Inc.
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

package io.cdap.cdap.runtime.spi.ssh;

import java.io.Closeable;
import java.io.IOException;

/**
 * Represents a SSH remote port forwarding.
 */
public interface RemotePortForwarding extends Closeable {

  /**
   * Returns the port that the remote host is listening on.
   */
  int getRemotePort();

  /**
   * Closes the remote port forwarding
   *
   * @throws IOException if failed to close the forwarding
   */
  @Override
  void close() throws IOException;
}
