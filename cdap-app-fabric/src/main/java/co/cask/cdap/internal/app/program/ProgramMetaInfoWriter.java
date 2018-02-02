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

package co.cask.cdap.internal.app.program;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * An interface that defines the behavior of
 */
class Solution {
  public int kEmptySlots(int[] flowers, int k) {
    if (flowers.length < 2) {
      return -1;
    }
    int[] slots = new int[flowers.length];
    for (int i = 0; i < flowers.length; i++) {
      slots[flowers[i]] = i;
    }
    for (int i = 0; i <= flowers.length - k; i++) {
      int min = Math.min(slots[i], slots[i + k - 1]);
      int max = Math.max(slots[i], slots[i + k - 1]);

    }
      return -1;
  }
}
