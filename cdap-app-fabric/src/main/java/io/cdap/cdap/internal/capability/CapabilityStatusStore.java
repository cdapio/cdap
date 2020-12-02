/*
 * Copyright © 2020 Cask Data, Inc.
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

package io.cdap.cdap.internal.capability;

import com.google.gson.Gson;
import com.google.inject.Inject;
import io.cdap.cdap.api.dataset.lib.CloseableIterator;
import io.cdap.cdap.spi.data.StructuredRow;
import io.cdap.cdap.spi.data.StructuredTable;
import io.cdap.cdap.spi.data.table.field.Field;
import io.cdap.cdap.spi.data.table.field.Fields;
import io.cdap.cdap.spi.data.table.field.Range;
import io.cdap.cdap.spi.data.transaction.TransactionRunner;
import io.cdap.cdap.spi.data.transaction.TransactionRunners;
import io.cdap.cdap.store.StoreDefinition;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * CapabilityStatusStore which takes care of reading , writing capability status and provides additional helpful methods
 */
public class CapabilityStatusStore implements CapabilityReader, CapabilityWriter {

  private static final Gson GSON = new Gson();
  private final TransactionRunner transactionRunner;

  @Inject
  CapabilityStatusStore(TransactionRunner transactionRunner) {
    this.transactionRunner = transactionRunner;
  }

  @Override
  public void checkAllEnabled(Collection<String> capabilities) throws IOException, CapabilityNotAvailableException {
    List<List<Field<?>>> multiKeys = new ArrayList<>();
    capabilities.forEach(capability -> multiKeys
      .add(Collections.singletonList(Fields.stringField(StoreDefinition.CapabilitiesStore.NAME_FIELD, capability))));
    Map<String, String> capabilityMap = TransactionRunners.run(transactionRunner, context -> {
      StructuredTable capabilityTable = context.getTable(StoreDefinition.CapabilitiesStore.CAPABILITIES);
      return capabilityTable.multiRead(multiKeys).stream().
        collect(Collectors.toMap(
          structuredRow -> structuredRow.getString(StoreDefinition.CapabilitiesStore.NAME_FIELD),
          structuredRow -> structuredRow.getString(StoreDefinition.CapabilitiesStore.STATUS_FIELD)
        ));

    }, IOException.class);
    for (String capability : capabilities) {
      if (!capabilityMap.containsKey(capability) || CapabilityStatus
        .valueOf(capabilityMap.get(capability).toUpperCase()) != CapabilityStatus.ENABLED) {
        throw new CapabilityNotAvailableException(capability);
      }
    }
  }

  @Override
  public Map<String, CapabilityConfig> getConfigs(Collection<String> capabilities) throws IOException {
    List<List<Field<?>>> multiKeys = new ArrayList<>();
    capabilities.forEach(capability -> multiKeys
      .add(Collections.singletonList(Fields.stringField(StoreDefinition.CapabilitiesStore.NAME_FIELD, capability))));
    return TransactionRunners.run(transactionRunner, context -> {
      StructuredTable capabilityTable = context.getTable(StoreDefinition.CapabilitiesStore.CAPABILITIES);
      return capabilityTable.multiRead(multiKeys).stream().
        collect(Collectors.toMap(
          structuredRow -> structuredRow.getString(StoreDefinition.CapabilitiesStore.NAME_FIELD),
          structuredRow -> GSON
            .fromJson(structuredRow.getString(StoreDefinition.CapabilitiesStore.CONFIG_FIELD), CapabilityConfig.class))
        );
    }, IOException.class);
  }

  /**
   * Returns list of all capability records
   *
   * @return
   * @throws IOException
   */
  public Map<String, CapabilityRecord> getCapabilityRecords() throws IOException {
    Map<String, CapabilityRecord> capabilityRecordMap = new HashMap<>();
    return TransactionRunners.run(transactionRunner, context -> {
      //query all operations
      Map<String, CapabilityOperationRecord> capabilityOperationMap = new HashMap<>();
      StructuredTable capabilityOperationTable = context
        .getTable(StoreDefinition.CapabilitiesStore.CAPABILITY_OPERATIONS);
      CloseableIterator<StructuredRow> operationsResultIterator = capabilityOperationTable
        .scan(Range.all(), Integer.MAX_VALUE);
      operationsResultIterator.forEachRemaining(structuredRow -> {
        String capability = structuredRow.getString(StoreDefinition.CapabilitiesStore.NAME_FIELD);
        CapabilityOperationRecord capabilityOperationRecord = new CapabilityOperationRecord(
          capability, CapabilityAction
          .valueOf(structuredRow.getString(StoreDefinition.CapabilitiesStore.ACTION_FIELD).toUpperCase()),
          GSON
            .fromJson(structuredRow.getString(StoreDefinition.CapabilitiesStore.CONFIG_FIELD), CapabilityConfig.class));
        capabilityOperationMap.put(capability, capabilityOperationRecord);
      });
      //query all capabilities
      StructuredTable capabilityTable = context.getTable(StoreDefinition.CapabilitiesStore.CAPABILITIES);
      CloseableIterator<StructuredRow> capabilityResultIterator = capabilityTable.scan(Range.all(), Integer.MAX_VALUE);
      capabilityResultIterator.forEachRemaining(structuredRow -> {
        String capability = structuredRow.getString(StoreDefinition.CapabilitiesStore.NAME_FIELD);
        CapabilityStatusRecord capabilityStatusRecord = new CapabilityStatusRecord(
          capability,
          CapabilityStatus
            .valueOf(structuredRow.getString(StoreDefinition.CapabilitiesStore.STATUS_FIELD).toUpperCase()),
          GSON
            .fromJson(structuredRow.getString(StoreDefinition.CapabilitiesStore.CONFIG_FIELD), CapabilityConfig.class));
        //add to result and remove from operations map
        capabilityRecordMap
          .put(capability,
               new CapabilityRecord(capabilityStatusRecord, capabilityOperationMap.remove(capability)));
      });
      //add the remaining operations to result
      capabilityOperationMap.keySet().forEach(capability -> capabilityRecordMap
        .put(capability, new CapabilityRecord(null, capabilityOperationMap.get(capability))));
      return capabilityRecordMap;
    }, IOException.class);
  }

  /**
   * Add or update capability
   *
   * @param capability
   * @param status
   * @throws IOException
   */
  @Override
  public void addOrUpdateCapability(String capability, CapabilityStatus status,
                                    CapabilityConfig config) throws IOException {
    TransactionRunners.run(transactionRunner, context -> {
      StructuredTable capabilityTable = context.getTable(StoreDefinition.CapabilitiesStore.CAPABILITIES);
      Collection<Field<?>> fields = new ArrayList<>();
      fields.add(Fields.stringField(StoreDefinition.CapabilitiesStore.NAME_FIELD, capability));
      fields.add(Fields.stringField(StoreDefinition.CapabilitiesStore.STATUS_FIELD, status.name().toLowerCase()));
      fields.add(Fields.stringField(StoreDefinition.CapabilitiesStore.CONFIG_FIELD, GSON.toJson(config)));
      fields.add(Fields.longField(StoreDefinition.CapabilitiesStore.UPDATED_TIME_FIELD, System.currentTimeMillis()));
      capabilityTable.upsert(fields);
    }, IOException.class);
  }

  /**
   * Delete capability
   *
   * @param capability
   * @throws IOException
   */
  @Override
  public void deleteCapability(String capability) throws IOException {
    TransactionRunners.run(transactionRunner, context -> {
      StructuredTable capabilityTable = context.getTable(StoreDefinition.CapabilitiesStore.CAPABILITIES);
      capabilityTable
        .delete(Collections.singleton(Fields.stringField(StoreDefinition.CapabilitiesStore.NAME_FIELD, capability)));
    }, IOException.class);
  }

  /**
   * Adds or update capability operations
   *
   * @param capability
   * @param actionType
   * @param config
   * @throws IOException
   */
  public void addOrUpdateCapabilityOperation(String capability, CapabilityAction actionType,
                                             CapabilityConfig config) throws IOException {
    TransactionRunners.run(transactionRunner, context -> {
      StructuredTable capabilityTable = context.getTable(StoreDefinition.CapabilitiesStore.CAPABILITY_OPERATIONS);
      Collection<Field<?>> fields = new ArrayList<>();
      fields.add(Fields.stringField(StoreDefinition.CapabilitiesStore.NAME_FIELD, capability));
      fields.add(Fields.stringField(StoreDefinition.CapabilitiesStore.ACTION_FIELD, actionType.name().toLowerCase()));
      fields.add(Fields.stringField(StoreDefinition.CapabilitiesStore.CONFIG_FIELD, GSON.toJson(config)));
      capabilityTable.upsert(fields);
    }, IOException.class);
  }

  /**
   * Deletes capability operations
   *
   * @param capability
   * @throws IOException
   */
  public void deleteCapabilityOperation(String capability) throws IOException {
    TransactionRunners.run(transactionRunner, context -> {
      StructuredTable capabilityTable = context.getTable(StoreDefinition.CapabilitiesStore.CAPABILITY_OPERATIONS);
      capabilityTable
        .delete(Collections.singleton(Fields.stringField(StoreDefinition.CapabilitiesStore.NAME_FIELD, capability)));
    }, IOException.class);
  }
}
