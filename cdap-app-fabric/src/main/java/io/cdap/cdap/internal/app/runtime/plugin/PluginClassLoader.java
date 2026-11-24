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
import io.cdap.cdap.api.artifact.CloseableClassLoader;
import io.cdap.cdap.app.program.ManifestFields;
import io.cdap.cdap.common.lang.CombineClassLoader;
import io.cdap.cdap.common.lang.DirectoryClassLoader;
import io.cdap.cdap.common.lang.PackageFilterClassLoader;
import io.cdap.cdap.internal.app.runtime.ProgramClassLoader;
import java.io.File;
import java.util.HashSet;
import java.util.Set;
import java.util.jar.Manifest;
import javax.annotation.Nullable;

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
 * The Plugin ClassLoader is then a URLClassLoader created by expanding the plugin bundle jar, with
 * the parent ClassLoader as the Combine ClassLoader.
 */
public class PluginClassLoader extends DirectoryClassLoader {

  private final ArtifactId artifactId;
  private final String topLevelJar;
  private final Set<String> exportPackages;

  /**
   * Creates a filtered parent {@link CloseableClassLoader} that exposes only {@code Export-Package}
   * classes from {@code templateClassLoader} alongside its filtered CDAP/program parent loader.
   */
  public static CloseableClassLoader createParent(ClassLoader templateClassLoader) {
    // Collect Export-Package declarations across any intermediate parent plugin DirectoryClassLoaders
    // as well as the root ProgramClassLoader so non-exported dependencies (such as Guava) stay hidden.
    Set<String> exportPackages = new HashSet<>();
    ClassLoader programClassLoader = templateClassLoader;
    while (programClassLoader != null
        && !(programClassLoader instanceof ProgramClassLoader)) {
      if (programClassLoader instanceof DirectoryClassLoader) {
        exportPackages.addAll(
            ManifestFields.getExportPackages(
                ((DirectoryClassLoader) programClassLoader).getManifest()));
      }
      programClassLoader = programClassLoader.getParent();
    }

    if (programClassLoader == null) {
      ClassLoader unwrapped = unwrapCloseable(templateClassLoader);
      if (unwrapped instanceof DirectoryClassLoader
          && unwrapCloseable(unwrapped.getParent()) instanceof CombineClassLoader) {
        DirectoryClassLoader dirClassLoader = (DirectoryClassLoader) unwrapped;
        Set<String> dirExportPackages =
            ManifestFields.getExportPackages(dirClassLoader.getManifest());
        PackageFilterClassLoader filteredDirClassLoader =
            new PackageFilterClassLoader(templateClassLoader, dirExportPackages::contains);
        CombineClassLoader combined =
            new CombineClassLoader(dirClassLoader.getParent(), filteredDirClassLoader);
        return new CloseableClassLoader(combined, filteredDirClassLoader);
      }
    }

    // This shouldn't happen
    Preconditions.checkArgument(programClassLoader != null,
        "Cannot find ProgramClassLoader");

    Manifest manifest = ((ProgramClassLoader) programClassLoader).getManifest();
    exportPackages.addAll(ManifestFields.getExportPackages(manifest));
    PackageFilterClassLoader filteredTemplateClassLoader =
        new PackageFilterClassLoader(templateClassLoader,
            exportPackages::contains);

    // The lib Classloader needs to be able to see all cdap api classes as well.
    // In this way, parent ClassLoader of the plugin ClassLoader will load class
    // from the parent of the template program class loader (which is a filtered
    // CDAP classloader), followed by template export-packages, then by a plugin
    // lib jars.
    CombineClassLoader classLoader = new CombineClassLoader(
        programClassLoader.getParent(), filteredTemplateClassLoader);
    // Creating a CloseableClassLoader ensures that the caller can close the
    // PackageFilterClassLoader that is held by the CombineClassLoader.
    return new CloseableClassLoader(classLoader, filteredTemplateClassLoader);
  }

  PluginClassLoader(ArtifactId artifactId, File directory, String topLevelJar, ClassLoader parent) {
    super(directory, parent, "lib");
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
   * Creates a new {@link ClassLoader} that only exposes classes in packages declared by
   * "Export-Package" in the manifest.
   */
  public ClassLoader getExportPackagesClassLoader() {
    return new PackageFilterClassLoader(this, exportPackages::contains);
  }

  /**
   * @return a file name of top level plugin jar. Main plugin class should reside in this jar.
   */
  public String getTopLevelJar() {
    return topLevelJar;
  }

  @Nullable
  private static ClassLoader unwrapCloseable(@Nullable ClassLoader classLoader) {
    ClassLoader current = classLoader;
    while (current instanceof CloseableClassLoader) {
      current = current.getParent();
    }
    return current;
  }
}
