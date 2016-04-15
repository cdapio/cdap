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
 * Helper methods for keys of {@link MetadataDataset}.
 */
// Note: these methods were refactored from MetadataDataset class. Once CDAP-3657 is fixed, these methods will need
// to be cleaned up CDAP-4291
final class KeyHelper {
  static void addTargetIdToKey(MDSKey.Builder builder, Id.NamespacedId namespacedId) {
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
    } else if (type.equals(Id.Stream.View.class.getSimpleName())) {
      Id.Stream.View view = (Id.Stream.View) namespacedId;
      String namespaceId = view.getNamespaceId();
      String streamId = view.getStreamId();
      String viewId = view.getId();
      builder.add(namespaceId);
      builder.add(streamId);
      builder.add(viewId);
    } else if (type.equals(Id.Artifact.class.getSimpleName())) {
      Id.Artifact artifactId = (Id.Artifact) namespacedId;
      String namespaceId = artifactId.getNamespace().getId();
      String name = artifactId.getName();
      String version = artifactId.getVersion().getVersion();
      builder.add(namespaceId);
      builder.add(name);
      builder.add(version);
    } else {
      throw new IllegalArgumentException("Illegal Type " + type + " of metadata source.");
    }
  }

  static Id.NamespacedId getTargetIdIdFromKey(MDSKey.Splitter keySplitter, String type) {
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
    } else if (type.equals(Id.Artifact.class.getSimpleName())) {
      String namespaceId = keySplitter.getString();
      String name = keySplitter.getString();
      String version = keySplitter.getString();
      return Id.Artifact.from(Id.Namespace.from(namespaceId), name, version);
    } else if (type.equals(Id.DatasetInstance.class.getSimpleName())) {
      String namespaceId = keySplitter.getString();
      String instanceId  = keySplitter.getString();
      return Id.DatasetInstance.from(namespaceId, instanceId);
    } else if (type.equals(Id.Stream.class.getSimpleName())) {
      String namespaceId = keySplitter.getString();
      String instanceId  = keySplitter.getString();
      return Id.Stream.from(namespaceId, instanceId);
    } else if (type.equals(Id.Stream.View.class.getSimpleName())) {
      String namespaceId = keySplitter.getString();
      String streamId  = keySplitter.getString();
      String viewId = keySplitter.getString();
      return Id.Stream.View.from(Id.Stream.from(namespaceId, streamId), viewId);
    }
    throw new IllegalArgumentException("Illegal Type " + type + " of metadata source.");
  }

  static String getTargetType(Id.NamespacedId namespacedId) {
    if (namespacedId instanceof Id.Program) {
      return Id.Program.class.getSimpleName();
    }
    return namespacedId.getClass().getSimpleName();
  }

  private KeyHelper() {
  }
}
