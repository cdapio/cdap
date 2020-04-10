/*
 * Copyright © 2014-2016 Cask Data, Inc.
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

package io.cdap.cdap.app.runtime;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import io.cdap.cdap.internal.app.runtime.BasicArguments;
import io.cdap.cdap.internal.app.runtime.ProgramOptionConstants;
import io.cdap.cdap.internal.app.runtime.SimpleProgramOptions;
import io.cdap.cdap.proto.Notification;
import io.cdap.cdap.proto.id.ProgramId;
import io.cdap.cdap.proto.id.ProgramRunId;

import java.lang.reflect.Type;
import java.util.Collections;
import java.util.Map;

/**
 * Represents options for a program execution.
 */
public interface ProgramOptions {

  /**
   * Returns the unique identifier for a program.
   */
  ProgramId getProgramId();

  /**
   * Returns the system arguments. It is for storing arguments used by the runtime system.
   */
  Arguments getArguments();

  /**
   * Returns the user arguments.
   */
  Arguments getUserArguments();

  /**
   * Returns {@code true} if executing in debug mode.
   */
  boolean isDebug();

  /**
   * Decodes {@link ProgramOptions} from a {@link Notification} object, based on the
   * {@link Notification.Type#PROGRAM_STATUS} type.
   */
  static ProgramOptions fromNotification(Notification notification, Gson gson) {
    Map<String, String> properties = notification.getProperties();
    ProgramId programId = gson.fromJson(properties.get(ProgramOptionConstants.PROGRAM_RUN_ID),
                                        ProgramRunId.class).getParent();

    String userArgumentsString = properties.get(ProgramOptionConstants.USER_OVERRIDES);
    String systemArgumentsString = properties.get(ProgramOptionConstants.SYSTEM_OVERRIDES);
    String debugString = properties.get(ProgramOptionConstants.DEBUG_ENABLED);

    Type stringStringMap = new TypeToken<Map<String, String>>() { }.getType();

    boolean debug = Boolean.parseBoolean(debugString);
    Map<String, String> userArguments = userArgumentsString == null ?
      Collections.emptyMap() : gson.fromJson(userArgumentsString, stringStringMap);
    Map<String, String> systemArguments = systemArgumentsString == null ?
      Collections.emptyMap() : gson.fromJson(systemArgumentsString, stringStringMap);

    return new SimpleProgramOptions(programId, new BasicArguments(systemArguments),
                                    new BasicArguments(userArguments), debug);
  }
}
