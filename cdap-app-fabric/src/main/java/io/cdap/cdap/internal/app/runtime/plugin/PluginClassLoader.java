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

package io.cdap.cdap.internal.app.runtime.plugin;

import com.google.common.base.Preconditions;
import io.cdap.cdap.api.artifact.ArtifactId;
import io.cdap.cdap.app.program.ManifestFields;
import io.cdap.cdap.common.lang.CombineClassLoader;
import io.cdap.cdap.common.lang.DirectoryClassLoader;
import io.cdap.cdap.common.lang.PackageFilterClassLoader;
import io.cdap.cdap.internal.app.runtime.ProgramClassLoader;

import java.io.File;
import java.util.Set;
import java.util.jar.Manifest;

/**
 * ClassLoader for template plugin. The ClassLoader hierarchy is pretty complicated.
 * <p/>
 * First, we have the "Plugin Lib ClassLoader".
 *
 * <pre>{@code
 *            CDAP System CL
 *                  ^
 *                  |
 *          Program Filter CL (cdap-api and hadoop classes only)
 *                  ^
 *                  |
 *             Template CL (expanded app bundle jar)
 *                  ^
 *                  |
 *       CombineCL of (Program Filter CL, Template Filter CL (Export-Package classes only))
 * }</pre>
 *
 * <p/>
 * The Plugin ClassLoader is then a URLClassLoader created by expanding the plugin bundle jar, with the parent
 * ClassLoader as the Combine ClassLoader.
 */
public class PluginClassLoader extends DirectoryClassLoader {

  private final ArtifactId artifactId;
  private final String topLevelJar;
  private final Set<String> exportPackages;

  static ClassLoader createParent(ClassLoader templateClassLoader) {
    // Find the ProgramClassLoader from the template ClassLoader
    ClassLoader programClassLoader = templateClassLoader;
    while (programClassLoader != null && !(programClassLoader instanceof ProgramClassLoader)) {
      programClassLoader = programClassLoader.getParent();
    }
    // This shouldn't happen
    Preconditions.checkArgument(programClassLoader != null, "Cannot find ProgramClassLoader");

    // Package filtered classloader of the template classloader, which only classes in "Export-Packages" are loadable.
    Manifest manifest = ((ProgramClassLoader) programClassLoader).getManifest();
    Set<String> exportPackages = ManifestFields.getExportPackages(manifest);
    ClassLoader filteredTemplateClassLoader = new PackageFilterClassLoader(templateClassLoader,
                                                                           exportPackages::contains);

    // The lib Classloader needs to be able to see all cdap api classes as well.
    // In this way, parent ClassLoader of the plugin ClassLoader will load class from the parent of the
    // template program class loader (which is a filtered CDAP classloader),
    // followed by template export-packages, then by a plugin lib jars.
    return new CombineClassLoader(programClassLoader.getParent(), filteredTemplateClassLoader);
  }

  PluginClassLoader(ArtifactId artifactId, File directory, String topLevelJar, ClassLoader parent) {
    super(directory, topLevelJar, parent, "lib");
    this.artifactId = artifactId;
    this.topLevelJar = topLevelJar;
    this.exportPackages = ManifestFields.getExportPackages(getManifest());
  }

  /**
   * @return the plugin's artifact id
   */
  public ArtifactId getArtifactId() {
    return artifactId;
  }

  /**
   * Creates a new {@link ClassLoader} that only exposes classes in packages declared by "Export-Package"
   * in the manifest.
   */
  public ClassLoader getExportPackagesClassLoader() {
    return new PackageFilterClassLoader(this, exportPackages::contains);
  }

  /**
   *
   * @return a file name of top level plugin jar. Main plugin class should reside in this jar.
   */
  public String getTopLevelJar() {
    return topLevelJar;
  }
}
