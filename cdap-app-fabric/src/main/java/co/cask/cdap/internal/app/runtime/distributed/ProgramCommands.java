/*
 * Copyright © 2014 Cask Data, Inc.
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

import co.cask.cdap.internal.app.runtime.ProgramOptionConstants;
import org.apache.twill.api.Command;

/**
 * The class carries {@link Command} that are used by the flow system.
 */
public final class ProgramCommands {

  public static final Command SUSPEND = Command.Builder.of("suspend").build();
  public static final Command RESUME = Command.Builder.of("resume").build();

  public static Command createSetInstances(int instances) {
    return Command.Builder.of(ProgramOptionConstants.INSTANCES).addOption("count", Integer.toString(instances)).build();
  }

  private ProgramCommands() {
  }
}
