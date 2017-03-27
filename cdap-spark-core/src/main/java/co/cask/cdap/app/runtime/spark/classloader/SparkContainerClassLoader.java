/*
 * Copyright © 2017 Cask Data, Inc.
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

package co.cask.cdap.app.runtime.spark.classloader;

import co.cask.cdap.common.app.MainClassLoader;
import com.google.common.base.Function;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import javax.annotation.Nullable;

/**
 * The {@link ClassLoader} for Spark containers in distributed mode.
 */
public class SparkContainerClassLoader extends MainClassLoader {

  private final SparkClassRewriter sparkClassRewriter;

  /**
   * Creates a new instance for the following set of {@link URL}.
   *
   * @param urls the URLs from which to load classes and resources
   * @param parent the parent classloader for delegation
   */
  public SparkContainerClassLoader(URL[] urls, ClassLoader parent) {
    super(urls, parent);
    this.sparkClassRewriter = new SparkClassRewriter(new Function<String, URL>() {
      @Nullable
      @Override
      public URL apply(String resourceName) {
        return findResource(resourceName);
      }
    }, false);
  }

  @Override
  protected boolean needIntercept(String className) {
    if (super.needIntercept(className)) {
      return true;
    }
    // There are certain Spark classes that need to be rewritten in distributed mode.
    // Just intercept all Spark classes and determine what actually needs to be rewritten
    // in the rewrite method.
    return className.startsWith("org.apache.spark.");
  }

  @Nullable
  @Override
  public byte[] rewriteClass(String className, InputStream input) throws IOException {
    byte[] rewrittenCode = super.rewriteClass(className, input);

    // If it is not a Spark class, just return
    if (!className.startsWith("org.apache.spark.")) {
      return rewrittenCode;
    }

    // Otherwise rewrite it using the SparkClassRewriter
    return sparkClassRewriter.rewriteClass(className,
                                           rewrittenCode == null ? input : new ByteArrayInputStream(rewrittenCode));
  }
}
