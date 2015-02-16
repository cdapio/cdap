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
 * WARRANTIES OR CONDITIONS OF ANY KIND, eitherap express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package co.cask.cdap.data2.dataset2.lib.table;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.dataset.DatasetSpecification;
import co.cask.cdap.api.dataset.lib.AbstractDataset;
import co.cask.cdap.api.dataset.lib.IndexedObjectStore;
import co.cask.cdap.api.dataset.module.EmbeddedDataset;
import co.cask.common.authorization.ACLEntry;
import co.cask.common.authorization.Permission;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.gson.Gson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Set;

/**
 * Simple dataset implementation of {@link co.cask.common.authorization.ACLStore}.
 *
 * TODO: Optimize
 */
public class ACLStoreTableDataset extends AbstractDataset implements ACLStoreTable {

  private static final Logger LOG = LoggerFactory.getLogger(ACLStoreTableDataset.class);
  private static final Gson GSON = new Gson();

  private final IndexedObjectStore<ACLEntry> store;

  public ACLStoreTableDataset(DatasetSpecification spec, @EmbeddedDataset("acls") IndexedObjectStore<ACLEntry> store) {
    super(spec.getName(), store);
    this.store = store;
  }

  @Override
  public void write(ACLEntry entry) throws Exception {
    byte[][] secondaryKeys = getSecondaryKeysForWrite(entry);
    store.write(getKey(entry), entry, secondaryKeys);
  }

  @Override
  public boolean exists(ACLEntry entry) throws Exception {
    if (entry.getPermission() == Permission.ANY) {
      List<ACLEntry> any = store.readAllByIndex(getKey(new ACLEntry(entry.getObject(), entry.getSubject(), null)));
      return !any.isEmpty();
    } else if (entry.getPermission() != Permission.ADMIN) {
      if (store.read(getKey(new ACLEntry(entry.getObject(), entry.getSubject(), Permission.ADMIN))) != null) {
        return true;
      }
    }

    return store.read(getKey(entry)) != null;
  }

  @Override
  public void delete(ACLEntry entry) throws Exception {
    store.delete(getKey(entry));
  }

  @Override
  public Set<ACLEntry> search(Iterable<Query> queries) throws Exception {
    // TODO: Optimize or use different underlying dataset to minimize number of calls
    Set<ACLEntry> result = Sets.newHashSet();
    for (Query query : queries) {
      if (query.getPermission() != null && query.getPermission() == Permission.ANY) {
        // return all ACL entries with permission that isn't ANY
        List<ACLEntry> entriesForAny = Lists.newArrayList();
        Query partialAnyQuery = new Query(query.getObjectId(), query.getSubjectId(), null);
        byte[] secondaryKeyForPartialAny = getKey(partialAnyQuery);
        List<ACLEntry> entriesForPartialAny = store.readAllByIndex(secondaryKeyForPartialAny);
        for (ACLEntry entry : entriesForPartialAny) {
          entriesForAny.add(new ACLEntry(entry.getObject(), entry.getSubject(), Permission.ANY));
        }
        result.addAll(entriesForAny);
      } else {
        if (query.getPermission() != null && query.getPermission() != Permission.ADMIN) {
          // non-ADMIN permission - ADMIN permission implies all other permissions,
          // so also add ACL entries if ADMIN permission exists
          Query queryForAdmin = new Query(query.getObjectId(), query.getSubjectId(), Permission.ADMIN);
          byte[] secondaryKeyForAdmin = getKey(queryForAdmin);
          List<ACLEntry> entriesForAdmin = store.readAllByIndex(secondaryKeyForAdmin);
          for (ACLEntry entry : entriesForAdmin) {
            result.add(new ACLEntry(entry.getObject(), entry.getSubject(), query.getPermission()));
          }
        }

        byte[] secondaryKey = getKey(query);
        List<ACLEntry> entries = store.readAllByIndex(secondaryKey);
        result.addAll(entries);
      }
    }
    return result;
  }

  @Override
  public void delete(Iterable<Query> queries) throws Exception {
    for (Query query : queries) {
      store.deleteAllByIndex(getKey(query));
    }
  }

  private byte[] getKey(ACLEntry entry) {
    return Bytes.toBytes(entry.toString());
  }

  private byte[] getKey(Query query) {
    return getKey(new ACLEntry(query.getObjectId(), query.getSubjectId(), query.getPermission()));
  }

  private byte[][] getSecondaryKeysForWrite(ACLEntry entry) {
    // TODO: sort of bad, but probably OK for now
    List<byte[]> keys = Lists.newArrayList();
    keys.add(getKey(new ACLEntry(entry.getObject(), null, null)));
    keys.add(getKey(new ACLEntry(entry.getObject(), entry.getSubject(), null)));
    keys.add(getKey(new ACLEntry(entry.getObject(), entry.getSubject(), entry.getPermission())));
    keys.add(getKey(new ACLEntry(entry.getObject(), null, entry.getPermission())));
    keys.add(getKey(new ACLEntry(null, entry.getSubject(), null)));
    keys.add(getKey(new ACLEntry(null, entry.getSubject(), entry.getPermission())));
    keys.add(getKey(new ACLEntry(null, null, entry.getPermission())));
    keys.add(getKey(new ACLEntry(null, null, null)));
    return keys.toArray(new byte[keys.size()][]);
  }
}
