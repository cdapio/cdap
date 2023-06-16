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

import java.util.Map;
import javax.annotation.Nullable;

/**
 * Details necessary for starting program.
 */
public class StartProgramEventDetails {

  private final String appId;
  private final String namespaceId;
  private final String programType;
  private final String programId;
  @Nullable
  private final Map<String, String> args;

  /**
   * Construct StartProgramEventDetails.
   *
   * @param appId AppId of program
   * @param namespaceId NamespaceId
   * @param programId ProgramId
   * @param programType ProgramType
   * @param args Map of args
   */
  public StartProgramEventDetails(String appId, String namespaceId, String programId,
                                   String programType,
                                   @Nullable Map<String, String> args) {
    this.appId = appId;
    this.namespaceId = namespaceId;
    this.programId = programId;
    this.args = args;
    this.programType = programType;
  }

  /**
   * Get App Id of program.
   *
   * @return appId;
   */
  public String getAppId() {
    return appId;
  }

  /**
   * Get args of program.
   *
   * @return args;
   */
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

  /**
   * Get Program Id of program.
   *
   * @return programId
   */
  public String getProgramId() {
    return programId;
  }

  /**
   * Get namespace Id of program.
   *
   * @return namespaceId
   */
  public String getNamespaceId() {
    return namespaceId;
  }

  /**
   * Get program type of program.
   *
   * @return programType
   */
  public String getProgramType() {
    return programType;
  }

}
