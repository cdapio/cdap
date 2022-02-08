/*
 * Copyright © 2014-2020 Cask Data, Inc.
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

package io.cdap.cdap;

import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.messaging.data.MessageId;

/**
 * This is helper class to make calls to SupportBundleHttpHandler methods directly.
 */
public class SupportBundleTestHelper {

  public static CConfiguration configuration;

  public static byte[] createSourceId(long sourceId) {
    byte[] buffer = new byte[MessageId.RAW_ID_SIZE];
    MessageId.putRawId(sourceId, (byte) 0, 0, (byte) 0, buffer, 0);
    return buffer;
  }
}

