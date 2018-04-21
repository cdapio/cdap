/*
 * Copyright © 2018 Cask Data, Inc.
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

package co.cask.cdap.runtime.spi.provisioner;

import co.cask.cdap.runtime.spi.ssh.SSHContext;

import java.util.Map;

/**
 * Context for provisioner operations.
 */
public interface ProvisionerContext {

  /**
   * @return the program run
   */
  ProgramRun getProgramRun();

  /**
   * Get the provisioner properties for this program run. These properties will start off as the provisioner properties
   * associated with the profile of the program run. The properties will then be overridden by any program preferences
   * that are prefixed with 'system.provisioner.', with the prefixed stripped. Those properties will then be
   * overridden by any runtime arguments or schedule properties that are prefixed with 'system.provisioner.', with
   * the prefixed stripped.
   *
   * @return the provisioner properties for the program run
   */
  Map<String, String> getProperties();

  /**
   * Returns the {@link SSHContext} for performing ssh operations.
   */
  SSHContext getSSHContext();
}
