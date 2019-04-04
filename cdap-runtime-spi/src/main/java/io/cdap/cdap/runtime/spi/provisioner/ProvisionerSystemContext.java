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
 *
 */

package io.cdap.cdap.runtime.spi.provisioner;

import java.util.Map;

/**
 * Context for system level provisioner information. System level information cannot be seen or modified by end
 * users.
 */
public interface ProvisionerSystemContext {

  /**
   * System properties are derived from the CDAP configuration. Anything in the CDAP configuration that is prefixed by
   * 'provisioner.system.properties.[provisioner-name].' will be adding as an entry in the system properties.
   * For example, if the provisioner is named 'abc', and there is a configuration property
   * 'provisioner.system.properties.abc.retry.timeout' with value '60', the system properties map will contain
   * a key 'retry.timeout' with value '60'. System properties are not visible to end users and cannot be overwritten
   * by end users.
   *
   * @return unmodifiable system properties for the provisioner
   */
  Map<String, String> getProperties();

  /**
   * Reloads system properties from the backing storage of the CDAP configuration.
   */
  void reloadProperties();

  /**
   * Returns the CDAP version
   */
  String getCDAPVersion();
}
