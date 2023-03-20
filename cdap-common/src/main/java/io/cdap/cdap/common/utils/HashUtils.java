/*
 * Copyright © 2023 Cask Data, Inc.
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

package io.cdap.cdap.common.utils;

import java.util.concurrent.TimeUnit;

/** Utility class for Hash-related operations. */
public class HashUtils {

  /**
   * Returns timed bucketed hash value. Therefore, for a given hash, result is identical as long as
   * call to this method has happened within the given window. This method also uses a jitter based
   * on provided hash to reduce likelihood of two time bucket with different hashes to be identical.
   * For a given n as the jitter chosen from 0 to window-1, time bucket windows will be as follows:
   * [window -n, 2*window -n), [2*window -n , 3*window -n), ... The beginning of an above time
   * bucket window which currentTime lies in uniquely identifies the time bucket window for the
   * given hash.
   *
   * @param hash value that needs to be time bucketed.
   * @param window in days
   * @param currentTime current time in millisecond
   */
  public static String timeBucketHash(String hash, int window, long currentTime) {
    // jitter is used to avoid having identical time bucket windows for different keys
    long jitter = TimeUnit.DAYS.toMillis(hash.hashCode() % window);
    long windowMillis = TimeUnit.DAYS.toMillis(window);

    long nextBucket = (currentTime / windowMillis) * windowMillis + windowMillis - jitter;
    return String.format("%s_%s", hash, nextBucket);
  }
}
