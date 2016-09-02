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
import co.cask.cdap.proto.id.ApplicationId;
import co.cask.cdap.proto.id.ArtifactId;
import co.cask.cdap.proto.id.DatasetId;
import co.cask.cdap.proto.id.NamespacedId;
import co.cask.cdap.proto.id.ProgramId;
import co.cask.cdap.proto.id.StreamId;
import co.cask.cdap.proto.id.StreamViewId;

import java.util.Arrays;

/**
 * Helper methods for keys of {@link MetadataDataset}.
 */
// Note: these methods were refactored from MetadataDataset class. Once CDAP-3657 is fixed, these methods will need
// to be cleaned up CDAP-4291
final class KeyHelper {
  static void addTargetIdToKey(MDSKey.Builder builder, NamespacedId namespacedId) {
    String type = getTargetType(namespacedId);
    if (type.equals(ProgramId.class.getSimpleName())) {
      ProgramId program = (ProgramId) namespacedId;
      String namespaceId = program.getNamespace();
      String appId = program.getApplication();
      String programType = program.getType().name();
      String programId = program.getProgram();
      builder.add(namespaceId);
      builder.add(appId);
      builder.add(programType);
      builder.add(programId);
    } else if (type.equals(ApplicationId.class.getSimpleName())) {
      ApplicationId application = (ApplicationId) namespacedId;
      String namespaceId = application.getNamespace();
      String instanceId = application.getApplication();
      builder.add(namespaceId);
      builder.add(instanceId);
    } else if (type.equals(DatasetId.class.getSimpleName())) {
      DatasetId datasetInstance = (DatasetId) namespacedId;
      String namespaceId = datasetInstance.getNamespace();
      String instanceId = datasetInstance.getDataset();
      builder.add(namespaceId);
      builder.add(instanceId);
    } else if (type.equals(StreamId.class.getSimpleName())) {
      StreamId stream = (StreamId) namespacedId;
      String namespaceId = stream.getNamespace();
      String instanceId = stream.getStream();
      builder.add(namespaceId);
      builder.add(instanceId);
    } else if (type.equals(StreamViewId.class.getSimpleName())) {
      StreamViewId view = (StreamViewId) namespacedId;
      String namespaceId = view.getNamespace();
      String streamId = view.getStream();
      String viewId = view.getView();
      builder.add(namespaceId);
      builder.add(streamId);
      builder.add(viewId);
    } else if (type.equals(ArtifactId.class.getSimpleName())) {
      ArtifactId artifactId = (ArtifactId) namespacedId;
      String namespaceId = artifactId.getNamespace();
      String name = artifactId.getArtifact();
      String version = artifactId.getVersion();
      builder.add(namespaceId);
      builder.add(name);
      builder.add(version);
    } else {
      throw new IllegalArgumentException("Illegal Type " + type + " of metadata source.");
    }
  }

  static NamespacedId getTargetIdIdFromKey(MDSKey.Splitter keySplitter, String type) {
    if (type.equals(ProgramId.class.getSimpleName())) {
      String namespaceId = keySplitter.getString();
      String appId = keySplitter.getString();
      String programType = keySplitter.getString();
      String programId = keySplitter.getString();
      return ProgramId.fromIdParts(Arrays.asList(namespaceId, appId, programType, programId));
    } else if (type.equals(ApplicationId.class.getSimpleName())) {
      String namespaceId = keySplitter.getString();
      String appId = keySplitter.getString();
      return ApplicationId.fromIdParts(Arrays.asList(namespaceId, appId));
    } else if (type.equals(ArtifactId.class.getSimpleName())) {
      String namespaceId = keySplitter.getString();
      String name = keySplitter.getString();
      String version = keySplitter.getString();
      return ArtifactId.fromIdParts(Arrays.asList(namespaceId, name, version));
    } else if (type.equals(DatasetId.class.getSimpleName())) {
      String namespaceId = keySplitter.getString();
      String instanceId  = keySplitter.getString();
      return DatasetId.fromIdParts(Arrays.asList(namespaceId, instanceId));
    } else if (type.equals(StreamId.class.getSimpleName())) {
      String namespaceId = keySplitter.getString();
      String instanceId  = keySplitter.getString();
      return StreamId.fromIdParts(Arrays.asList(namespaceId, instanceId));
    } else if (type.equals(StreamViewId.class.getSimpleName())) {
      String namespaceId = keySplitter.getString();
      String streamId  = keySplitter.getString();
      String viewId = keySplitter.getString();
      return StreamViewId.fromIdParts(Arrays.asList(namespaceId, streamId, viewId));
    }
    throw new IllegalArgumentException("Illegal Type " + type + " of metadata source.");
  }

  static String getTargetType(NamespacedId namespacedId) {
    if (namespacedId instanceof ProgramId) {
      return ProgramId.class.getSimpleName();
    }
    return namespacedId.getClass().getSimpleName();
  }

  private KeyHelper() {
  }
}
