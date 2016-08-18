/*
 * Copyright © 2016 Cask Data, Inc.
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

package co.cask.cdap.internal.app.runtime.distributed;

import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.proto.id.ProgramId;
import com.google.common.base.Preconditions;

import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.annotation.Nullable;

/**
 * Utility class to convert to and from the Twill application name for a {@link ProgramId}, since Twill application
 * name is a simple String and must encode all identifying information of a program.
 */
public final class TwillAppNames {

  // Pattern to split a Twill App name into [type].[namespaceId].[appName].[programName]
  private static final Pattern APP_NAME_PATTERN = Pattern.compile("^(\\S+)\\.(\\S+)\\.(\\S+)\\.(\\S+)$");

  private TwillAppNames() { }

  /**
   * Returns the Twill app name to be used for a Twill app launched for a given program.
   */
  static String toTwillAppName(ProgramId programId) {
    return String.format("%s.%s.%s.%s", programId.getType().name().toLowerCase(),
                         programId.getNamespace(), programId.getApplication(), programId.getProgram());
  }

  /**
   * Given a Twill app name, returns the id of the program that was used to construct this Twill app name.
   *
   * @throws IllegalArgumentException if the given app name does not match the {@link #APP_NAME_PATTERN}.
   */
  public static ProgramId fromTwillAppName(String twillAppName) {
    return fromTwillAppName(twillAppName, true);
  }

  /**
   * Given a Twill app name, returns the id of the program that was used to construct this Twill app name.
   * @return {@code null} if mustMatch is false, and if the specified Twill app name does
   * not match the {@link #APP_NAME_PATTERN}.
   * For instance, for the Constants.Service.MASTER_SERVICES Twill app, it will return null.
   *
   * @throws IllegalArgumentException if the given app name does not match the {@link #APP_NAME_PATTERN}
   *                                  and mustMatch is true.
   */
  @Nullable
  static ProgramId fromTwillAppName(String twillAppName, boolean mustMatch) {
    Matcher matcher = APP_NAME_PATTERN.matcher(twillAppName);
    if (!matcher.matches()) {
      Preconditions.checkArgument(!mustMatch, "Twill app name '%s' does not match pattern for programs", twillAppName);
      return null;
    }
    // this exception shouldn't happen (unless someone changes the pattern), because the pattern has 4 groups
    Preconditions.checkArgument(4 == matcher.groupCount(),
                                "Expected matcher for '%s' to have 4 groups, but it had %s groups.",
                                twillAppName, matcher.groupCount());
    ProgramType type = ProgramType.valueOf(matcher.group(1).toUpperCase());
    return new ProgramId(matcher.group(2), matcher.group(3), type, matcher.group(4));
  }
}
