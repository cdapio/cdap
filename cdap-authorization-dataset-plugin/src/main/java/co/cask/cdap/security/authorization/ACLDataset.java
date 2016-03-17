/*
 * Copyright © 2015-2016 Cask Data, Inc.
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
package co.cask.cdap.security.authorization;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.dataset.lib.AbstractDataset;
import co.cask.cdap.api.dataset.table.Row;
import co.cask.cdap.api.dataset.table.Scanner;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.proto.id.EntityId;
import co.cask.cdap.proto.security.Action;
import co.cask.cdap.proto.security.Principal;
import co.cask.cdap.proto.security.Privilege;

import java.util.HashSet;
import java.util.Set;

/**
 * System dataset for storing ACLs for authorization.
 *
 * Key format:
 * [entity][principal-type][principal-name][action].
 *
 * E.g.
 * [NAMESPACE:myspace][ROLE][admins][READ]
 */
class ACLDataset extends AbstractDataset {

  static final String TABLE_NAME = "acls";
  private static final byte[] VALUE_COLUMN = new byte[0];

  private final Table table;

  ACLDataset(Table table) {
    super(TABLE_NAME, table);
    this.table = table;
  }

  /**
   * Search the dataset to retrieve the set of allowed {@link Action} for the given {@link EntityId} and
   * {@link Principal}.
   *
   * @param entity the entity
   * @param principal the principal
   * @return the set of actions allowed for the user on the entity
   */
  public Set<Action> search(EntityId entity, Principal principal) {
    Set<Action> result = new HashSet<>();

    ACLDatasetKey mdsKey = getKey(entity, principal);
    byte[] startKey = mdsKey.getKey();
    byte[] stopKey = Bytes.stopKeyForPrefix(startKey);
    Scanner scan = table.scan(startKey, stopKey);

    try {
      Row next;
      while ((next = scan.next()) != null) {
        byte[] value = next.get(VALUE_COLUMN);
        if (value == null) {
          continue;
        }
        result.add(Action.valueOf(Bytes.toString(value)));
      }
    } finally {
      scan.close();
    }

    return result;
  }

  /**
   * Add an {@link Action} on the specified {@link EntityId} for the given {@link Principal}.
   */
  public void add(EntityId entity, Principal principal, Action action) {
    table.put(getKey(entity, principal, action).getKey(), VALUE_COLUMN, Bytes.toBytes(action.name()));
  }

  /**
   * Remove an {@link Action} on the specified {@link EntityId} from the given {@link Principal}.
   */
  public void remove(EntityId entity, Principal principal, Action action) {
    table.delete(getKey(entity, principal, action).getKey());
  }

  /**
   * Remove all {@link Action actions} on the specified {@link EntityId} for the given {@link Principal}.
   */
  public void remove(EntityId entity, Principal principal) {
    ACLDatasetKey aclDatasetKey = getKey(entity, principal);
    byte[] startKey = aclDatasetKey.getKey();
    byte[] stopKey = Bytes.stopKeyForPrefix(startKey);
    Scanner scan = table.scan(startKey, stopKey);

    try {
      Row next;
      while ((next = scan.next()) != null) {
        table.delete(next.getRow());
      }
    } finally {
      scan.close();
    }
  }

  /**
   * Remove all {@link Action actions} for all {@link Principal principals} on the specified {@link EntityId}.
   */
  public void remove(EntityId entity) {
    ACLDatasetKey aclDatasetKey = getKey(entity);
    byte[] startKey = aclDatasetKey.getKey();
    byte[] stopKey = Bytes.stopKeyForPrefix(startKey);
    Scanner scan = table.scan(startKey, stopKey);

    try {
      Row next;
      while ((next = scan.next()) != null) {
        table.delete(next.getRow());
      }
    } finally {
      scan.close();
    }
  }

  /**
   * List all the {@link Privilege privileges} for the specified {@link Principal}.
   */
  public Set<Privilege> listPrivileges(Principal principal) {
    Set<Privilege> privileges = new HashSet<>();
    // scan the whole table
    Scanner scan = table.scan(null, null);
    try {
      Row next;
      while ((next = scan.next()) != null) {
        Principal curPrincipal = getPrincipal(next.getRow());
        if (curPrincipal.equals(principal)) {
          byte[] value = next.get(VALUE_COLUMN);
          if (value == null) {
            continue;
          }
          privileges.add(new Privilege(getEntity(next.getRow()), Action.valueOf(Bytes.toString(value))));
        }
      }
    } finally {
      scan.close();
    }
    return privileges;
  }

  private ACLDatasetKey getKey(EntityId entity) {
    return getKeyBuilder(entity).build();
  }

  private ACLDatasetKey getKey(EntityId entity, Principal principal) {
    return getKeyBuilder(entity, principal).build();
  }

  private ACLDatasetKey getKey(EntityId entity, Principal principal, Action action) {
    return getKeyBuilder(entity, principal, action).build();
  }

  private ACLDatasetKey.Builder getKeyBuilder(EntityId entity, Principal principal, Action action) {
    return getKeyBuilder(entity, principal).add(action.name());
  }

  private ACLDatasetKey.Builder getKeyBuilder(EntityId entity, Principal principal) {
    return getKeyBuilder(entity).add(principal.getType().name()).add(principal.getName());
  }

  private ACLDatasetKey.Builder getKeyBuilder(EntityId entity) {
    return new ACLDatasetKey.Builder().add(entity.toString());
  }

  private EntityId getEntity(byte[] rowKey) {
    ACLDatasetKey.Splitter keySplitter = new ACLDatasetKey(rowKey).split();
    // The rowkey is [entity][principal-type][principal-name][action-name]
    return EntityId.fromString(keySplitter.getString());
  }

  private Principal getPrincipal(byte[] rowKey) {
    ACLDatasetKey.Splitter keySplitter = new ACLDatasetKey(rowKey).split();
    // The rowkey is [entity][principal-type][principal-name][action-name]
    keySplitter.skipString(); // skip the entity
    String principalType = keySplitter.getString();
    String principalName = keySplitter.getString();
    return new Principal(principalName, Principal.PrincipalType.valueOf(principalType.toUpperCase()));
  }
}
