package com.continuuity.hive.guice;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Enumeration;
import java.util.jar.Manifest;

/**
 * Custom classloader to load explore/hive classes.
 */
public class CustomURLClassLoader extends URLClassLoader {
  public CustomURLClassLoader(URL[] urls, ClassLoader classLoader) {
    super(urls, classLoader);
  }

  @Override
  protected Class<?> findClass(String s) throws ClassNotFoundException {
    return super.findClass(s);
  }

  @Override
  protected Package definePackage(String s, Manifest manifest, URL url) throws IllegalArgumentException {
    return super.definePackage(s, manifest, url);
  }

  @Override
  public URL findResource(String s) {
    return super.findResource(s);
  }

  @Override
  public Enumeration<URL> findResources(String s) throws IOException {
    return super.findResources(s);
  }

  @Override
  public Class<?> loadClass(String s) throws ClassNotFoundException {
    return super.loadClass(s);
  }

  @Override
  protected synchronized Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
    if (name.startsWith("com.continuuity.hive.")) {
      return customLoad(name, resolve);
    }

    return super.loadClass(name, resolve);
  }

  private Class<?> customLoad(String name, boolean resolve) throws ClassNotFoundException {
    Class<?> cls = findClass(name);
    if (resolve) {
      resolveClass(cls);
    }
    return cls;
  }

  @Override
  public URL getResource(String s) {
    return super.getResource(s);
  }

  @Override
  public Enumeration<URL> getResources(String s) throws IOException {
    return super.getResources(s);
  }

  @Override
  public InputStream getResourceAsStream(String s) {
    return super.getResourceAsStream(s);
  }

  @Override
  protected Package definePackage(String s, String s2, String s3, String s4, String s5, String s6, String s7, URL url)
    throws IllegalArgumentException {
    return super.definePackage(s, s2, s3, s4, s5, s6, s7, url);
  }

  @Override
  protected Package getPackage(String s) {
    return super.getPackage(s);
  }

  @Override
  protected Package[] getPackages() {
    return super.getPackages();
  }

  @Override
  protected String findLibrary(String s) {
    return super.findLibrary(s);
  }

  @Override
  public synchronized void setDefaultAssertionStatus(boolean b) {
    super.setDefaultAssertionStatus(b);
  }

  @Override
  public synchronized void setPackageAssertionStatus(String s, boolean b) {
    super.setPackageAssertionStatus(s, b);
  }

  @Override
  public synchronized void setClassAssertionStatus(String s, boolean b) {
    super.setClassAssertionStatus(s, b);
  }

  @Override
  public synchronized void clearAssertionStatus() {
    super.clearAssertionStatus();
  }
}
