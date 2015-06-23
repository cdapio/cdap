/*
 * Copyright © 2012-2014 Cask Data, Inc.
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

package co.cask.cdap.cli.completer.element;

import co.cask.cdap.cli.completer.StringsCompleter;
import co.cask.cdap.client.ApplicationClient;
import co.cask.cdap.common.UnauthorizedException;
import co.cask.cdap.proto.ApplicationRecord;
import com.google.common.base.Supplier;
import com.google.common.collect.Lists;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import javax.inject.Inject;

/**
 * Completer for application IDs.
 */
public class AppIdCompleter extends StringsCompleter {

  @Inject
  public AppIdCompleter(final ApplicationClient applicationClient) {
    super(new Supplier<Collection<String>>() {
      @Override
      public Collection<String> get() {
        try {
          List<ApplicationRecord> appsList = applicationClient.list();
          List<String> appIds = Lists.newArrayList();
          for (ApplicationRecord item : appsList) {
            appIds.add(item.getName());
          }
          return appIds;
        } catch (IOException e) {
          return Lists.newArrayList();
        } catch (UnauthorizedException e) {
          return Lists.newArrayList();
        }
      }
    });
  }
}
