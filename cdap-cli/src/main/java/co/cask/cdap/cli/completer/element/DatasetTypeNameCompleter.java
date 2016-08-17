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

package co.cask.cdap.cli.completer.element;

import co.cask.cdap.cli.CLIConfig;
import co.cask.cdap.cli.completer.StringsCompleter;
import co.cask.cdap.client.DatasetTypeClient;
import co.cask.cdap.common.UnauthenticatedException;
import co.cask.cdap.proto.DatasetTypeMeta;
import co.cask.cdap.security.spi.authorization.UnauthorizedException;
import com.google.common.base.Function;
import com.google.common.base.Supplier;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import javax.inject.Inject;

/**
 * Completer for dataset types.
 */
public class DatasetTypeNameCompleter extends StringsCompleter {

  @Inject
  public DatasetTypeNameCompleter(final DatasetTypeClient datasetTypeClient,
                                  final CLIConfig cliConfig) {
    super(new Supplier<Collection<String>>() {
      @Override
      public Collection<String> get() {
        try {
          List<DatasetTypeMeta> list = datasetTypeClient.list(cliConfig.getCurrentNamespace());
          return Lists.newArrayList(
            Iterables.transform(list, new Function<DatasetTypeMeta, String>() {
              @Override
              public String apply(DatasetTypeMeta input) {
                return input.getName();
              }
            })
          );
        } catch (IOException | UnauthenticatedException | UnauthorizedException e) {
          return new ArrayList<>();
        }
      }
    });
  }
}
