/*
 * Copyright 2014 Cask, Inc.
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

package co.cask.cdap.common.lang.jar;

import co.cask.cdap.api.annotation.ExposeClass;
import com.google.common.base.Predicate;

import java.io.File;
import java.lang.annotation.Annotation;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;

/**
 * Classloader that loads the given class, checks if it has {@link co.cask.cdap.api.annotation.ExposeClass} annotation
 * if it does not have the annotation , it throws {@link java.lang.ClassNotFoundException}
 */
public class ExposeFilterClassLoader extends ClassLoader {
  private final URL[] datasetUrls;
  private URLClassLoader datasetClassLoader;
  private Predicate<String> exposeAnnotations;


  public ExposeFilterClassLoader(File datasetTypeJar, ClassLoader parentClassLoader,
                                 Predicate<String> exposeAnnotations) throws MalformedURLException {
    super(parentClassLoader);
    this.datasetUrls = ClassPathUrlsUtil.getClassPathUrls(datasetTypeJar);
    this.datasetClassLoader = new URLClassLoader(datasetUrls, parentClassLoader);
    this.exposeAnnotations = exposeAnnotations;
  }

  @Override
  public Class<?> findClass(String name) throws ClassNotFoundException {
    Class<?> dataset = datasetClassLoader.loadClass(name);

    for (Annotation classAnnotation : dataset.getAnnotations()) {
      if (exposeAnnotations.apply(classAnnotation.annotationType().getName())) {
        return dataset;
      }
    }
    throw new ClassNotFoundException(String.format("Unable to find class %s", name));
  }
}
