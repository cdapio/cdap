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
import io.cdap.cdap.common.UnauthenticatedException;
import io.cdap.cdap.proto.ApplicationRecord;
import io.cdap.cdap.security.spi.authorization.UnauthorizedException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import javax.inject.Inject;

/**
 * Completer for application IDs.
 */
public class AppIdCompleter extends StringsCompleter {

  @Inject
  public AppIdCompleter(final ApplicationClient applicationClient, final CLIConfig cliConfig) {
    super(new Supplier<Collection<String>>() {
      @Override
      public Collection<String> get() {
        try {
          List<ApplicationRecord> appsList = applicationClient.list(cliConfig.getCurrentNamespace());
          List<String> appIds = new ArrayList<>();
          for (ApplicationRecord item : appsList) {
            appIds.add(item.getName());
          }
          return appIds;
        } catch (IOException | UnauthenticatedException | UnauthorizedException e) {
          return new ArrayList<>();
        }
      }
    });
  }
}
