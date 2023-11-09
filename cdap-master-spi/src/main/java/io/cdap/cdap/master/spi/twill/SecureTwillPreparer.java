/*
 * Copyright © 2021 Cask Data, Inc.
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

package io.cdap.cdap.master.spi.twill;

import org.apache.twill.api.TwillPreparer;
import org.apache.twill.api.TwillRunnable;

/**
 * Extension interface for {@link TwillPreparer} to implement if it supports execution of a {@link
 * TwillRunnable} under secure contexts.
 */
public interface SecureTwillPreparer extends TwillPreparer {

  /**
   * Runs the given runnable with the specified secret disks.
   *
   * @param runnableName name of the {@link TwillRunnable}
   * @param secretDisks the secret disks to use
   * @return this {@link TwillPreparer}
   */
  SecureTwillPreparer withSecretDisk(String runnableName, SecretDisk... secretDisks);

  /**
   * Runs the given runnable with provided {@link SecurityContext}
   *
   * @param runnableName name of the {@link TwillRunnable}
   * @param securityContext the security context to be used
   * @return this {@link TwillPreparer}
   */
  SecureTwillPreparer withSecurityContext(String runnableName,
      SecurityContext securityContext);

  /**
   * Runs the given runnable with namespace workload identity,
   * this feature removes the GOOGLE_APPLICATION_CREDENTIALS environment variable
   * to enable namespaced service accounts.
   *
   * @param runnableName name of the {@link TwillRunnable}
   * @return this {@link TwillPreparer}
   */
  SecureTwillPreparer withNamespacedWorkloadIdentity(String runnableName);
}
