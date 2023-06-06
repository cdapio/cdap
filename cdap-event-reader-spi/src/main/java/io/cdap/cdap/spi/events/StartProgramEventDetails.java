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

package io.cdap.cdap.spi.events;

import javax.annotation.Nullable;
import java.util.Map;

public class StartProgramEventDetails {

  private final String appId;
  private final String namespaceId;
  private final String programType;
  private final String programId;
  @Nullable
  private final Map<String, String> args;

  private StartProgramEventDetails(String appId, String namespaceId, String programId,
                                   String programType,
                                   @Nullable Map<String, String> args) {
    this.appId = appId;
    this.namespaceId = namespaceId;
    this.programId = programId;
    this.args = args;
    this.programType = programType;
  }


  public String getAppId() {
    return appId;
  }

  @Nullable
  public Map<String, String> getArgs() {
    return args;
  }


  @Override
  public String toString() {
    return "ProgramStatusEventDetails{"
        + "appId='" + appId + '\''
        + ", namespaceId='" + namespaceId + '\''
        + ", programType='" + programType + '\''
        + ", programId='" + programId + '\''
        + ", userArgs=" + args
        + '}';
  }

  public String getProgramId() {
    return programId;
  }

  public String getNamespaceId() {
    return namespaceId;
  }

  public String getProgramType() {
    return programType;
  }

}
