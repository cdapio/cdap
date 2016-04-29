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

package co.cask.cdap.internal.app.runtime.artifact;

import co.cask.cdap.api.Config;
import co.cask.cdap.api.app.Application;
import co.cask.cdap.api.artifact.ArtifactId;
import co.cask.cdap.api.artifact.ArtifactScope;
import co.cask.cdap.internal.lang.Reflections;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.id.NamespaceId;
import com.google.common.base.Preconditions;
import com.google.common.reflect.TypeToken;

import java.lang.reflect.Type;

/**
 * Util class that contains helper methods related to handling of artifacts.
 */
public final class Artifacts {

  public static String getFileName(ArtifactId artifactId) {
    return String.format("%s-%s-%s.jar", artifactId.getScope(), artifactId.getName(), artifactId.getVersion());
  }

  /**
   * Resolve the application's config type.
   *
   * @param appClass the application class to resolve the config type for
   * @return the resolved config type
   * @throws IllegalArgumentException if the config type is not a valid type
   */
  public static Type getConfigType(Class<? extends Application> appClass) {
    TypeToken<?> configType = TypeToken.of(appClass).resolveType(Application.class.getTypeParameters()[0]);
    if (Reflections.isResolved(configType.getType())) {
      // Default the type to Config.class if the resolved type is not subclass of Config.
      // It normally won't happen, unless someone generate the bytecode directly.
      // This is for Scala. If the user don't specify any type parameter, Scala will automatically insert
      // "scala.runtime.Nothing$" as the type parameter.
      if (Config.class.isAssignableFrom(configType.getRawType())) {
        return configType.getType();
      }
      return Config.class;
    }

    // It has to be Config
    Preconditions.checkArgument(Config.class == configType.getRawType(),
      "Application config type " + configType + " not supported. " +
      "Type must extend Config and cannot be parameterized.");
    return Config.class;
  }

  /**
   * Converts a {@link ArtifactId} to {@link co.cask.cdap.proto.id.ArtifactId}.
   *
   * @param namespaceId the user namespace to use
   * @param artifactId the artifact id to convert
   */
  public static co.cask.cdap.proto.id.ArtifactId toArtifactId(NamespaceId namespaceId, ArtifactId artifactId) {
    ArtifactScope scope = artifactId.getScope();
    NamespaceId artifactNamespace = scope == ArtifactScope.SYSTEM ? NamespaceId.SYSTEM : namespaceId;
    return artifactNamespace.artifact(artifactId.getName(), artifactId.getVersion().getVersion());
  }

  private Artifacts() {
    // Prevent instantiation of util class.
  }
}
