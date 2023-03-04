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
package io.cdap.cdap.master.upgrade;

import com.google.common.base.Throwables;
import java.io.IOException;
import org.apache.hadoop.security.UserGroupInformation;

/**
 * Provides a user ID to use for upgrade and post-upgrade jobs based on the {@link
 * UserGroupInformation}.
 */
public class UpgradeUserIDProvider {

  /**
   * @return The user ID obtained from the UGI.
   */
  public static String getUserID() {
    // For backwards compatibility, the upgrade job should assume the same local identity as other pods.
    // This may not work if Kerberos is enabled in the cluster.
    String userId;
    try {
      userId = UserGroupInformation.getCurrentUser().getShortUserName();
    } catch (IOException e) {
      throw Throwables.propagate(e);
    }
    return userId;
  }
}
