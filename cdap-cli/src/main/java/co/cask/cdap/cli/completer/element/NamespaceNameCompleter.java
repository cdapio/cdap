/*
 * Copyright © 2015 Cask Data, Inc.
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
import co.cask.cdap.client.NamespaceClient;
import co.cask.cdap.common.UnauthorizedException;
import co.cask.cdap.proto.NamespaceMeta;
import com.google.common.base.Supplier;
import com.google.common.collect.Lists;
import com.google.inject.Inject;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Completer for namespace ids.
 */
public class NamespaceNameCompleter extends StringsCompleter {

  @Inject
  public NamespaceNameCompleter(final NamespaceClient namespaceClient) {
    super(new Supplier<Collection<String>>() {
      @Override
      public Collection<String> get() {
        List<String> namespaceIds = new ArrayList<>();
        try {
          for (NamespaceMeta namespaceMeta : namespaceClient.list()) {
            namespaceIds.add(namespaceMeta.getName());
          }
        } catch (IOException e) {
          return Lists.newArrayList();
        } catch (UnauthorizedException e) {
          return Lists.newArrayList();
        }
        return namespaceIds;
      }
    });
  }
}
