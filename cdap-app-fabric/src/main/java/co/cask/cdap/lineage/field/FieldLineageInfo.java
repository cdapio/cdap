/*
 * Copyright © 2018 Cask Data, Inc.
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

package co.cask.cdap.lineage.field;

import co.cask.cdap.api.lineage.field.EndPoint;
import co.cask.cdap.api.lineage.field.InputField;
import co.cask.cdap.api.lineage.field.Operation;
import co.cask.cdap.api.lineage.field.ReadOperation;
import co.cask.cdap.api.lineage.field.TransformOperation;
import co.cask.cdap.api.lineage.field.WriteOperation;
import co.cask.cdap.lineage.field.codec.OperationTypeAdapter;
import com.google.common.base.Charsets;
import com.google.common.collect.Sets;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Class representing the information about field lineage for a single program run.
 * Currently we store the operations associated with the field lineage and corresponding
 * checksum. Algorithm to compute checksum is same as how Avro computes the Schema fingerprint.
 * (https://issues.apache.org/jira/browse/AVRO-1006). The implementation of fingerprint
 * algorithm is taken from {@code org.apache.avro.SchemaNormalization} class. Since the checksum
 * is persisted in store, any change to the canonicalize form or fingerprint algorithm would
 * require upgrade step to update the stored checksums.
 */
public class FieldLineageInfo {
  private static final Gson GSON = new GsonBuilder()
    .registerTypeAdapter(Operation.class, new OperationTypeAdapter())
    .create();

  private final Set<Operation> operations;
  private long checksum;

  /**
   * Create an instance of a class from supplied collection of operations.
   * Validations are performed on the collection before creating instance. All of the operations
   * must have unique names. Collection must have at least one operation of type READ and one
   * operation of type WRITE. The origins specified for the {@link InputField} are also validated
   * to make sure the operation with the corresponding name exists in the collection. However, we do not
   * validate the existence of path to the fields in the destination from sources. If no such path exists
   * then the lineage will be incomplete.
   *
   * @param operations the collection of field lineage operations
   * @throws IllegalArgumentException if validation fails
   */
  public FieldLineageInfo(Collection<? extends Operation> operations) {
    Set<String> operationNames = new HashSet<>();
    Set<String> allOrigins = new HashSet<>();
    boolean readExists = false;
    boolean writeExists = false;
    for (Operation operation : operations) {
      if (!operationNames.add(operation.getName())) {
        throw new IllegalArgumentException(String.format("All operations provided for creating field " +
                                                           "level lineage info must have unique names. " +
                                                           "Operation name '%s' is repeated.", operation.getName()));
      }
      switch (operation.getType()) {
        case READ:
          ReadOperation read = (ReadOperation) operation;
          EndPoint source = read.getSource();
          if (source == null) {
            throw new IllegalArgumentException(String.format("Source endpoint cannot be null for the read " +
                                                               "operation '%s'.", read.getName()));
          }
          readExists = true;
          break;
        case TRANSFORM:
          TransformOperation transform = (TransformOperation) operation;
          allOrigins.addAll(transform.getInputs().stream().map(InputField::getOrigin).collect(Collectors.toList()));
          break;
        case WRITE:
          WriteOperation write = (WriteOperation) operation;
          EndPoint destination = write.getDestination();
          if (destination == null) {
            throw new IllegalArgumentException(String.format("Destination endpoint cannot be null for the write " +
                                                               "operation '%s'.", write.getName()));
          }
          allOrigins.addAll(write.getInputs().stream().map(InputField::getOrigin).collect(Collectors.toList()));
          writeExists = true;
          break;
        default:
          // no-op
      }
    }

    if (!readExists) {
      throw new IllegalArgumentException("Field level lineage requires at least one operation of type 'READ'.");
    }

    if (!writeExists) {
      throw new IllegalArgumentException("Field level lineage requires at least one operation of type 'WRITE'.");
    }

    Sets.SetView<String> invalidOrigins = Sets.difference(allOrigins, operationNames);
    if (!invalidOrigins.isEmpty()) {
      throw new IllegalArgumentException(String.format("No operation is associated with the origins '%s'.",
                                                       invalidOrigins));
    }

    this.operations = new HashSet<>(operations);
    this.checksum = computeChecksum();
  }

  /**
   * @return the checksum for the operations
   */
  public long getChecksum() {
    return checksum;
  }

  /**
   * @return the operations
   */
  public Set<Operation> getOperations() {
    return operations;
  }

  private long computeChecksum() {
    return fingerprint64(canonicalize().getBytes(Charsets.UTF_8));
  }

  /**
   * Creates the canonicalize representation of the collection of operations. Canonicalize representation is
   * simply the JSON format of operations. Before creating the JSON, collection of operations is sorted based
   * on the operation name so that irrespective of the order of insertion, same set of operations always generate
   * same canonicalize form. This representation is then used for computing the checksum. So if there are any changes
   * to this representation, upgrade step would be required to update all the checksums stored in store.
   */
  private String canonicalize() {
    List<Operation> ops = new ArrayList<>(operations);
    Collections.sort(ops, new Comparator<Operation>() {
      @Override
      public int compare(Operation o1, Operation o2) {
        return o1.getName().compareTo(o2.getName());
      }
    });
    return GSON.toJson(ops);
  }

  private static final long EMPTY64 = 0xc15d213aa4d7a795L;

  /**
   * The implementation of fingerprint algorithm is copied from {@code org.apache.avro.SchemaNormalization} class.
   *
   * @param data byte string for which fingerprint is to be computed
   * @return the 64-bit Rabin Fingerprint (as recommended in the Avro spec) of a byte string
   */
   private long fingerprint64(byte[] data) {
    long result = EMPTY64;
    for (byte b: data) {
      int index = (int) (result ^ b) & 0xff;
      result = (result >>> 8) ^ FP64.FP_TABLE[index];
    }
    return result;
  }

  /* An inner class ensures that FP_TABLE initialized only when needed. */
  private static class FP64 {
    private static final long[] FP_TABLE = new long[256];
    static {
      for (int i = 0; i < 256; i++) {
        long fp = i;
        for (int j = 0; j < 8; j++) {
          long mask = -(fp & 1L);
          fp = (fp >>> 1) ^ (EMPTY64 & mask);
        }
        FP_TABLE[i] = fp;
      }
    }
  }
}
