/*
 * Copyright © 2014 Cask Data, Inc.
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

package co.cask.cdap.pipeline;

import co.cask.cdap.api.dataset.Dataset;
import co.cask.cdap.common.lang.ApiResourceListHolder;
import co.cask.cdap.common.lang.ClassLoaders;
import co.cask.cdap.common.lang.jar.BundleJarUtil;
import co.cask.cdap.common.utils.DirUtils;
import co.cask.cdap.data2.dataset2.SingleTypeModule;
import com.google.common.base.Objects;
import com.google.common.io.Files;
import com.google.common.reflect.TypeToken;
import org.apache.twill.filesystem.LocalLocationFactory;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;

import java.io.File;
import java.io.IOException;

/**
 * Abstract implementation of {@link Stage} allowing ability to determine type
 * to invoke the actual processing of event.
 *
 * @param <T> Type of object processed by this stage.
 */
public abstract class AbstractStage<T> implements Stage {
  protected static final int DEFAULT_MODULE_VERSION = 1;

  private Context ctx;
  private TypeToken<T> typeToken;

  /**
   * Constructor that allows determining the type {@link Stage} is looking to process.
   *
   * @param typeToken instance to determine type of data {@link Stage} is processing.
   */
  protected AbstractStage(TypeToken<T> typeToken) {
    this.typeToken = typeToken;
  }

  /**
   * Processes an object passed to it from context.
   *
   * @param ctx of processing.
   */
  @SuppressWarnings("unchecked")
  public final void process(Context ctx) throws Exception {
    this.ctx = ctx;
    Object upStream = ctx.getUpStream();
    if (typeToken.getRawType().isAssignableFrom(upStream.getClass())) {
      process((T) typeToken.getRawType().cast(upStream));
    }
  }

  /**
   * Emits the object to send to next {@link Stage} in processing.
   *
   * @param o to be emitted to downstream
   */
  protected final void emit(Object o) {
    ctx.setDownStream(o);
  }

  /**
   * Abstract process that does a safe cast to the type.
   *
   * @param o Object to be processed which is of type T
   */
  public abstract void process(T o) throws Exception;

  /**
   * Method to instantiate a dataset type and get version of the dataset, for modules, this will return the
   * {@link #DEFAULT_MODULE_VERSION}
   * @param location of archive
   * @param typeName Dataset type name
   * @return int - version of dataset type
   * @throws IOException
   */
  protected int getDatasetVersion(Location location, String typeName)
    throws IOException {
    LocationFactory lf = new LocalLocationFactory();
    File tempDir = Files.createTempDir();
    try {
      ClassLoader parentClassLoader = Objects.firstNonNull(Thread.currentThread().getContextClassLoader(),
                                                           AbstractStage.class.getClassLoader());

      BundleJarUtil.unpackProgramJar(lf.create(location.toURI()), tempDir);
      Class<?> datasetType;
      try {
        ClassLoader programClassLoader = ClassLoaders.newProgramClassLoader(tempDir,
                                                                            ApiResourceListHolder.getResourceList(),
                                                                            parentClassLoader);
        datasetType = programClassLoader.loadClass(typeName);
      } catch (ClassNotFoundException e) {
        // we cannot load the type from JAR, if the type is a part of module, it cannot be loaded,
        // we are using this only internally, so returning DEFAULT_MODULE_VERSION.
        return  DEFAULT_MODULE_VERSION;
      }
      if (Dataset.class.isAssignableFrom(datasetType)) {
        SingleTypeModule module = new SingleTypeModule((Class<? extends Dataset>) datasetType);
        return module.getVersion();
      }
    } catch (Exception e) {
      return DEFAULT_MODULE_VERSION;
    } finally {
      DirUtils.deleteDirectoryContents(tempDir);
    }
    return DEFAULT_MODULE_VERSION;
  }
}
