/*
 * Copyright 2012-2014 Cask, Inc.
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

package co.cask.cdap.shell.completer.element;

import co.cask.cdap.client.ApplicationClient;
import co.cask.cdap.client.exception.UnAuthorizedAccessTokenException;
import co.cask.cdap.proto.ProgramRecord;
import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.shell.completer.StringsCompleter;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.Lists;
import com.google.inject.Inject;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Completer for program IDs.
 */
public abstract class AllProgramIdCompleter extends StringsCompleter {

  private final ApplicationClient appClient;

  @Inject
  public AllProgramIdCompleter(final ApplicationClient appClient) {
    this.appClient = appClient;
  }

  @Override
  protected Supplier<Collection<String>> getStringsSupplier() {
    return Suppliers.memoizeWithExpiration(new Supplier<Collection<String>>() {
      @Override
      public Collection<String> get() {
        try {
          Map<ProgramType, List<ProgramRecord>> programMap = appClient.listAllPrograms();
          List<String> programIds = Lists.newArrayList();
          for (Collection<ProgramRecord> programs : programMap.values()) {
            for (ProgramRecord programRecord : programs) {
              programIds.add(programRecord.getApp() + "." + programRecord.getId());
            }
          }
          return programIds;
        } catch (Exception e) {
          return Lists.newArrayList();
        }
      }
    }, 3, TimeUnit.SECONDS);
  }
}
