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

package co.cask.cdap.namespace;

import java.util.ArrayList;
import java.util.List;

/**
 * Stores/Retrieves namespace metadata from memory
 * Only for initial testing. Should be removed eventually
 */
public class InMemoryNamespaceMetaStore implements NamespaceMetaStore {
  private List<NamespaceMetadata> table = new ArrayList<NamespaceMetadata>();

  @Override
  public void create(String name, String displayName, String description) throws NamespaceAlreadyExistsException {
    if (exists(name)) {
      throw new NamespaceAlreadyExistsException("Namespace '" + name + "' already exists");
    }
    table.add(new NamespaceMetadata.Builder().setName(name).setDisplayName(displayName).setDescription(description)
                .build());
  }

  @Override
  public NamespaceMetadata get(String name) {
    NamespaceMetadata ns = null;
    for (NamespaceMetadata each : table) {
      if (name.equals(each.getName())) {
        ns = each;
        break;
      }
    }
    return ns;
  }

  @Override
  public void delete(String name) throws NamespaceNotFoundException {
    NamespaceMetadata ns = get(name);
    if (null == ns) {
      throw new NamespaceNotFoundException("Namespace '" + name + "' does not exist");
    }
    table.remove(ns);
  }

  @Override
  public List<NamespaceMetadata> list() {
    return table;
  }

  @Override
  public boolean exists(String name) {
    return get(name) != null;
  }

  /**
   *
   */
  public class NamespaceNotFoundException extends Exception {
    public NamespaceNotFoundException(String s) {
      super(s);
    }
  }

  /**
   *
   */
  public class NamespaceAlreadyExistsException extends Exception {
    public NamespaceAlreadyExistsException(String s) {
      super(s);
    }
  }
}
