/*
 * Copyright © 2017 Cask Data, Inc.
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

package co.cask.cdap.internal.app.runtime.schedule.store;

import co.cask.cdap.proto.id.ApplicationId;
import com.google.common.base.Joiner;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Helper class that contains methods that aid with scheduler data format upgrade.
 */
public final class ScheduleUpgradeUtil {

  private ScheduleUpgradeUtil() {
  }

  public static String splitAndRemoveDefaultVersion(String origString, int lengthCheck, int versionIdx) {
    String[] splits = origString.split(":");
    if (splits.length != lengthCheck) {
      return null;
    }

    if (!splits[versionIdx].equals(ApplicationId.DEFAULT_VERSION)) {
      return null;
    }

    List<String> splitArray = new ArrayList<>(Arrays.asList(splits));
    splitArray.remove(versionIdx);
    return Joiner.on(":").join(splitArray);
  }

  public static String getNameWithDefaultVersion(String[] splits, int index) {
    List<String> splitsList = new ArrayList<>(Arrays.asList(splits));
    splitsList.add(index, ApplicationId.DEFAULT_VERSION);
    return Joiner.on(":").join(splitsList);
  }
}
