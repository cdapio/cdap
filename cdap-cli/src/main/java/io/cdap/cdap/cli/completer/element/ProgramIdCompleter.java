/*
 * Copyright © 2012-2016 Cask Data, Inc.
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

package io.cdap.cdap.cli.completer.element;

import com.google.common.base.Supplier;
import io.cdap.cdap.cli.CLIConfig;
import io.cdap.cdap.cli.completer.StringsCompleter;
import io.cdap.cdap.client.ApplicationClient;
import io.cdap.cdap.proto.ProgramRecord;
import io.cdap.cdap.proto.ProgramType;
import io.cdap.cdap.security.spi.authentication.UnauthenticatedException;
import io.cdap.cdap.security.spi.authorization.UnauthorizedException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Completer for program IDs.
 */
public class ProgramIdCompleter extends StringsCompleter {

  public ProgramIdCompleter(final ApplicationClient appClient,
                            final CLIConfig cliConfig,
                            final ProgramType programType) {
    super(new Supplier<Collection<String>>() {
      @Override
      public Collection<String> get() {
        try {
          List<ProgramRecord> programs = appClient.listAllPrograms(cliConfig.getCurrentNamespace(), programType);
          List<String> programIds = new ArrayList<>();
          for (ProgramRecord programRecord : programs) {
            programIds.add(programRecord.getApp() + "." + programRecord.getName());
          }
          return programIds;
        } catch (IOException | UnauthenticatedException | UnauthorizedException e) {
          return new ArrayList<>();
        }
      }
    });
  }
}
