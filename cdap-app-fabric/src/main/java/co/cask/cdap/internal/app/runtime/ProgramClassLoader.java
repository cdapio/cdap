/*
 * Copyright © 2016 Cask Data, Inc.
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

package co.cask.cdap.internal.app.runtime;

import co.cask.cdap.api.dataset.Dataset;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.dataset.DatasetClassRewriter;
import co.cask.cdap.common.lang.ClassLoaders;
import co.cask.cdap.common.lang.DirectoryClassLoader;
import co.cask.cdap.internal.asm.Classes;
import com.google.common.base.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;

/**
 * ClassLoader that implements bundle jar feature, in which the application jar contains
 * its dependency jars inside.
 */
public class ProgramClassLoader extends DirectoryClassLoader {

  private static final Logger LOG = LoggerFactory.getLogger(ProgramClassLoader.class);

  private final File dir;
  private final Function<String, URL> classResourceLookup;
  private final Map<String, Boolean> datasetClassCache;
  private final DatasetClassRewriter datasetClassRewriter;

  /**
   * Constructs an instance that load classes from the given directory.
   * <p/>
   * The URLs for class loading are:
   * <p/>
   * <pre>
   * [dir]
   * [dir]/*.jar
   * [dir]/lib/*.jar
   * </pre>
   */
  public ProgramClassLoader(CConfiguration cConf, File dir, ClassLoader parent) {
    super(cConf, dir, parent, "lib");
    this.dir = dir;
    this.classResourceLookup = ClassLoaders.createClassResourceLookup(this);
    this.datasetClassCache = new HashMap<>();
    this.datasetClassRewriter = new DatasetClassRewriter();
  }

  /**
   * Returns the directory that this classloader is used to load class resources from.
   */
  public File getDir() {
    return dir;
  }

  @Override
  protected boolean needIntercept(String className) {
    try {
      return Classes.isSubTypeOf(className, Dataset.class.getName(), classResourceLookup, datasetClassCache);
    } catch (Exception e) {
      // This shouldn't happen. Won't propagate the exception since this call happen during classloading.
      // If there is IOException in reading class resource, the classloading should fail by itself.
      LOG.error("Unexpected exception when inspecting class '" + className + "'", e);
      return false;
    }
  }

  @Override
  public byte[] rewriteClass(String className, InputStream input) throws IOException {
    return datasetClassRewriter.rewriteClass(className, input);
  }
}
