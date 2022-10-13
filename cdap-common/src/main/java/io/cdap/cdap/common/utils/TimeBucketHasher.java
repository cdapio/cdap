/*
 * Copyright © 2022 Cask Data, Inc.
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

/**
 * Utility class for generating time bucketed hashes
 */
public class TimeBucketHasher {


  /**
   * Return timed bucketed hash value. Therefore, for a given hash, result is identical as long as call to this
   * method has happened within the given window.
   * This method also uses a jitter based on provided key to reduces likelihood of two time bucket with different keys
   * to be identical.
   *
   * @param hash        value that needs to be time bucketed.
   * @param window      in days
   * @param currentTime current time in millisecond
   * @return
   */
  public static String timeBucketHash(String hash, int window, long currentTime) {
    // jitter is used to avoid having identical time bucket windows for different keys
    long jitter = TimeUnit.DAYS.toMillis(hash.hashCode() % window);
    long windowMSec = TimeUnit.DAYS.toMillis(window);

    long nextBucket = (currentTime / windowMSec) * windowMSec + windowMSec - jitter;
    return String.format("%s_%s", hash, nextBucket);
  }
}
