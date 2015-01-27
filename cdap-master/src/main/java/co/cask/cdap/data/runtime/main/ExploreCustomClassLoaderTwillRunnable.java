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

package co.cask.cdap.data.runtime.main;

import co.cask.cdap.common.lang.ClassLoaders;
import co.cask.cdap.common.lang.InstantiatorFactory;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Throwables;
import com.google.common.reflect.TypeToken;
import org.apache.twill.api.AbstractTwillRunnable;
import org.apache.twill.api.Command;
import org.apache.twill.api.TwillContext;
import org.apache.twill.api.TwillRunnable;
import org.apache.twill.api.TwillRunnableSpecification;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URL;
import java.net.URLClassLoader;

/**
 * Used to load a {@link ExploreServiceTwillRunnable} using {@link CustomResourcesClassLoader}. This loads
 * config files bundled with the application and not config files in the classpath used to start the application.
 * This is required as on some clusters the classpath used to start a container contains a stripped down version of 
 * Hadoop config files.
 */
public class ExploreCustomClassLoaderTwillRunnable extends AbstractTwillRunnable {
  private static final Logger LOG = LoggerFactory.getLogger(ExploreCustomClassLoaderTwillRunnable.class);
  private static final String CLASS_NAME = ExploreServiceTwillRunnable.class.getName();
  
  private final TwillRunnableSpecification twillRunnableSpecification;
  private TwillRunnable twillRunnable;
  private ClassLoader customClassLoader;

  public ExploreCustomClassLoaderTwillRunnable(TwillRunnableSpecification twillRunnableSpecification) {
    this.twillRunnableSpecification = twillRunnableSpecification;
  }

  @Override
  public TwillRunnableSpecification configure() {
    return twillRunnableSpecification;
  }

  @Override
  public void initialize(TwillContext context) {
    super.initialize(context);
    
    if (getClass().getClassLoader() instanceof URLClassLoader) {
      LOG.info("Using CustomResourcesClassLoader to load config files bundled with application.");
      customClassLoader =
        new CustomResourcesClassLoader(((URLClassLoader) getClass().getClassLoader()).getURLs(),
                                       getClass().getClassLoader());
    } else {
      LOG.warn("Classloader is not URLCLassLoader, config files bundled with application might not be loaded.");
      customClassLoader = getClass().getClassLoader();
    }
    
    ClassLoader previousClassLoader = ClassLoaders.setContextClassLoader(customClassLoader);

    try {
      @SuppressWarnings("unchecked") 
      Class<? extends TwillRunnable> twillRunnableClass = 
        (Class<? extends TwillRunnable>) customClassLoader.loadClass(CLASS_NAME);
      twillRunnable = new InstantiatorFactory(false).get(TypeToken.of(twillRunnableClass)).create();
      twillRunnable.initialize(context);
    } catch (Exception e) {
      throw Throwables.propagate(e);
    } finally {
      ClassLoaders.setContextClassLoader(previousClassLoader);
    }
  }

  @Override
  public void handleCommand(Command command) throws Exception {
    ClassLoader previousClassLoader = ClassLoaders.setContextClassLoader(customClassLoader);
    try {
      twillRunnable.handleCommand(command);
    } finally {
      ClassLoaders.setContextClassLoader(previousClassLoader);
    }
  }

  @Override
  public void stop() {
    ClassLoader previousClassLoader = ClassLoaders.setContextClassLoader(customClassLoader);
    try {
      twillRunnable.stop();
    } finally {
      ClassLoaders.setContextClassLoader(previousClassLoader);
    }
  }

  @Override
  public void destroy() {
    ClassLoader previousClassLoader = ClassLoaders.setContextClassLoader(customClassLoader);
    try {
      twillRunnable.destroy();
    } finally {
      ClassLoaders.setContextClassLoader(previousClassLoader);
    }
  }

  @Override
  public void run() {
    ClassLoader previousClassLoader = ClassLoaders.setContextClassLoader(customClassLoader);
    try {
      twillRunnable.run();
    } finally {
      ClassLoaders.setContextClassLoader(previousClassLoader);
    }
  }

  /**
   * A custom {@link ClassLoader} used to load config files bundled with a {@link TwillRunnable}.
   */
  @VisibleForTesting
  static class CustomResourcesClassLoader extends URLClassLoader {
    private final ClassLoader twillClassLoader;

    public CustomResourcesClassLoader(URL[] urls, ClassLoader twillClassLoader) {
      super(urls, ClassLoader.getSystemClassLoader().getParent());
      this.twillClassLoader = twillClassLoader;
    }

    @Override
    public Class<?> loadClass(String s) throws ClassNotFoundException {
      // Load all Twill API classes from parent classloader, since they are already loaded by parent classloader.
      if (s.startsWith("org.apache.twill.api.")) {
        return twillClassLoader.loadClass(s);
      }

      return super.loadClass(s);
    }
  }
}
