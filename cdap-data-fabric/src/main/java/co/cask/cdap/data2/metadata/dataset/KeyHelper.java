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

package co.cask.cdap.data2.metadata.dataset;

import co.cask.cdap.data2.dataset2.lib.table.MDSKey;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.ProgramType;

/**
 * Helper methods for keys of {@link BusinessMetadataDataset}.
 */
public class KeyHelper {
  public static void addNamespaceIdToKey(MDSKey.Builder builder, Id.NamespacedId namespacedId) {
    String type = getTargetType(namespacedId);
    if (type.equals(Id.Program.class.getSimpleName())) {
      Id.Program program = (Id.Program) namespacedId;
      String namespaceId = program.getNamespaceId();
      String appId = program.getApplicationId();
      String programType = program.getType().name();
      String programId = program.getId();
      builder.add(namespaceId);
      builder.add(appId);
      builder.add(programType);
      builder.add(programId);
    } else if (type.equals(Id.Application.class.getSimpleName())) {
      Id.Application application = (Id.Application) namespacedId;
      String namespaceId = application.getNamespaceId();
      String instanceId = application.getId();
      builder.add(namespaceId);
      builder.add(instanceId);
    } else if (type.equals(Id.DatasetInstance.class.getSimpleName())) {
      Id.DatasetInstance datasetInstance = (Id.DatasetInstance) namespacedId;
      String namespaceId = datasetInstance.getNamespaceId();
      String instanceId = datasetInstance.getId();
      builder.add(namespaceId);
      builder.add(instanceId);
    } else if (type.equals(Id.Stream.class.getSimpleName())) {
      Id.Stream stream = (Id.Stream) namespacedId;
      String namespaceId = stream.getNamespaceId();
      String instanceId = stream.getId();
      builder.add(namespaceId);
      builder.add(instanceId);
    } else {
      throw new IllegalArgumentException("Illegal Type " + type + " of metadata source.");
    }
  }

  public static Id.NamespacedId getNamespaceIdFromKey(MDSKey.Splitter keySplitter, String type) {
    if (type.equals(Id.Program.class.getSimpleName())) {
      String namespaceId = keySplitter.getString();
      String appId = keySplitter.getString();
      String programType = keySplitter.getString();
      String programId = keySplitter.getString();
      return Id.Program.from(namespaceId, appId, ProgramType.valueOf(programType), programId);
    } else if (type.equals(Id.Application.class.getSimpleName())) {
      String namespaceId = keySplitter.getString();
      String appId = keySplitter.getString();
      return Id.Application.from(namespaceId, appId);
    } else if (type.equals(Id.DatasetInstance.class.getSimpleName())) {
      String namespaceId = keySplitter.getString();
      String instanceId  = keySplitter.getString();
      return Id.DatasetInstance.from(namespaceId, instanceId);
    } else if (type.equals(Id.Stream.class.getSimpleName())) {
      String namespaceId = keySplitter.getString();
      String instanceId  = keySplitter.getString();
      return Id.Stream.from(namespaceId, instanceId);
    }
    throw new IllegalArgumentException("Illegal Type " + type + " of metadata source.");
  }

  public static String getTargetType(Id.NamespacedId namespacedId) {
    if (namespacedId instanceof Id.Program) {
      return Id.Program.class.getSimpleName();
    }
    return namespacedId.getClass().getSimpleName();
  }
}
