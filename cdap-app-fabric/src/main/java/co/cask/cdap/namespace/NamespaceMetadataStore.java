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

import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.data2.dataset2.lib.table.MetadataStoreDataset;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;

/**
 * Store for namespace metadata
 */
public class NamespaceMetadataStore extends MetadataStoreDataset {

  private static final String TYPE_NAMESPACE_META = "namespaceMeta";
  public NamespaceMetadataStore(Table table) {
    super(table);
  }

  public List<NamespaceMetadata> getAllNamespaces() {
    return list(new Key.Builder().add(TYPE_NAMESPACE_META).build(), NamespaceMetadata.class);
  }

  public void writeNamespace(String name, String displayName, String description) {
    write(new Key.Builder().add(TYPE_NAMESPACE_META, name).build(), new NamespaceMetadata(name, displayName, description));
  }

  public void deleteNamespace(String name) {
    deleteAll(new Key.Builder().add(TYPE_NAMESPACE_META, name).build());
  }

  /**
   * Temporary store till integration with MDS is complete
   */
  public static class InMemoryNamespaceMetadataStore {
    private List<NamespaceMetadata> table = new ArrayList<NamespaceMetadata>();
    private static InMemoryNamespaceMetadataStore store = new InMemoryNamespaceMetadataStore();

    private InMemoryNamespaceMetadataStore() {

    }

    public static InMemoryNamespaceMetadataStore getInstance() {
      return store;
    }

    public List<NamespaceMetadata> getAllNamespaces() {
      return table;
    }

    public void writeNamespace(String name, String displayName, String description) throws
      NamespaceAlreadyExistsException {
      if (exists(name)) {
        throw new NamespaceAlreadyExistsException("Namespace '" + name + "' already exists");
      }
      table.add(new NamespaceMetadata(name, displayName, description));
    }

    public void deleteNamespace(String name) throws NamespaceNotFoundException {
      NamespaceMetadata ns = get(name);
      if (null == ns) {
        throw new NamespaceNotFoundException("Namespace '" + name + "' does not exist");
      }
      table.remove(ns);
    }

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

    private boolean exists(String name) {
      return get(name) != null;
    }

    /**
     *
     */
    public class NamespaceNotFoundException extends Throwable {
      public NamespaceNotFoundException(String s) {
        super(s);
      }
    }

    /**
     *
     */
    public class NamespaceAlreadyExistsException extends Throwable {
      public NamespaceAlreadyExistsException(String s) {
        super(s);
      }
    }
  }
}
