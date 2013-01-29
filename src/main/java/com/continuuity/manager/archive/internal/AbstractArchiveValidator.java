package com.continuuity.manager.archive.internal;

import com.continuuity.common.classloader.JarClassLoader;
import com.continuuity.common.classloader.JarResourceException;
import com.continuuity.common.classloader.JarResources;
import com.continuuity.common.utils.StackTraceUtil;
import com.continuuity.manager.archive.ArchiveValidator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.jar.Attributes;
import java.util.jar.Manifest;

/**
 *
 */
public abstract class AbstractArchiveValidator<T> implements ArchiveValidator {

  private static final Logger LOG = LoggerFactory.getLogger(AbstractArchiveValidator.class);

  protected abstract Status doValidate(File archive, Class<? extends T> mainClass, ClassLoader classLoader) throws IllegalArgumentException;

  protected abstract Class<T> getClassType();

  @Override
  public final Status validate(File archive) throws IllegalArgumentException {
    try {
      JarResources jarResources = new JarResources(archive.getAbsolutePath());
      ClassLoader classLoader = new JarClassLoader(jarResources);

      // Try to extract Main-Class from manifest file
      Manifest manifest = jarResources.getManifest();
      if (manifest == null) {
        throw debugAndThrow("No manifest file in archive: %s", archive);
      }

      String className = manifest.getMainAttributes().getValue(Attributes.Name.MAIN_CLASS);
      if (className == null) {
        throw debugAndThrow("No main class in archive: %s", archive);
      }

      // Load the main class
      Class<?> mainClass = null;
      LOG.trace("Attempting to load class '{}'", className);
      try {
        mainClass = classLoader.loadClass(className);
        return doValidate(archive, mainClass.asSubclass(getClassType()), classLoader);

      } catch (ClassNotFoundException e) {
        throw debugAndThrow("Unable to load %s from manifest in archive %s. Reason: %s. %s",
                            className, archive, e.getMessage(), StackTraceUtil.toStringStackTrace(e));
      } catch (ClassCastException e) {
        throw debugAndThrow("Main class %s is not subclass of %s. %s", mainClass, getClassType(), StackTraceUtil.toStringStackTrace(e));
      }

    } catch (JarResourceException e) {
      throw debugAndThrow("Failed to load archive %s. Reason: %s. %s",
                          archive, e.getMessage(), StackTraceUtil.toStringStackTrace(e));
    }
  }

  protected IllegalArgumentException debugAndThrow(String fmt, Object...args) {
    String message = String.format(fmt, args);
    LOG.debug(message);
    throw new IllegalArgumentException(message);
  }
}
