/*
 * Copyright © 2014 Cask Data, Inc.
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

package io.cdap.cdap.security.auth;

import com.google.inject.Inject;
import io.cdap.cdap.common.conf.CConfiguration;

import java.io.IOException;

/**
 * Maintains secret keys in memory and uses them to sign and validate authentication tokens.
 */
public class InMemoryKeyManager extends MapBackedKeyManager {

  /**
   * Create an InMemoryKeyManager that stores keys in memory only.
   */
  @Inject
  InMemoryKeyManager(CConfiguration conf) {
    super(conf);
  }

  @Override
  public void doInit() throws IOException {
    generateKey();
  }

  @Override
  public void shutDown() {
    // nothing to do
  }
}
