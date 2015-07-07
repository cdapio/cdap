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

package co.cask.cdap.internal.app.runtime.adapter;

import co.cask.cdap.app.program.ManifestFields;
import co.cask.cdap.common.lang.CombineClassLoader;
import co.cask.cdap.common.lang.DirectoryClassLoader;
import co.cask.cdap.common.lang.PackageFilterClassLoader;
import co.cask.cdap.common.lang.ProgramClassLoader;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableList;

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
 *                  ^
 *                  |
 *           Plugin Lib CL (Common library for plugin)
 * }</pre>
 *
 * <p/>
 * The Plugin ClassLoader is then a URLClassLoader created by expanding the plugin bundle jar, with the parent
 * ClassLoader as the Plugin Lib ClassLoader.
 */
public class PluginClassLoader extends DirectoryClassLoader {

  private final Set<String> exportPackages;

  /**
   * Creates the parent ClassLoader for the plugin ClassLoader. See javadoc of this class for details.
   */
  static ClassLoader createParent(File pluginLibDir, ClassLoader templateClassLoader) {
    // Includes all jars in the plugins/template/lib directory
    return new DirectoryClassLoader(pluginLibDir, createParent(templateClassLoader));
  }

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
                                                                           Predicates.in(exportPackages));

    // The lib Classloader needs to be able to see all cdap api classes as well.
    // In this way, parent ClassLoader of the plugin ClassLoader will load class from the parent of the
    // template program class loader (which is a filtered CDAP classloader),
    // followed by template export-packages, then by a plugin lib jars.
    return new CombineClassLoader(programClassLoader.getParent(), ImmutableList.of(filteredTemplateClassLoader));
  }

  PluginClassLoader(File directory, ClassLoader parent) {
    super(directory, parent, "lib");
    this.exportPackages = ManifestFields.getExportPackages(getManifest());
  }

  /**
   * Creates a new {@link ClassLoader} that only exposes classes in packages declared by "Export-Package"
   * in the manifest.
   */
  public ClassLoader getExportPackagesClassLoader() {
    return new PackageFilterClassLoader(this, Predicates.in(exportPackages));
  }
}
