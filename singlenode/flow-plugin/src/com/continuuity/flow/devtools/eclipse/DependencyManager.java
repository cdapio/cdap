package com.continuuity.flow.devtools.eclipse;


import com.continuuity.flow.devtools.eclipse.templates.FileTemplateConfig;
import org.eclipse.core.runtime.CoreException;
import org.eclipse.core.runtime.IPath;
import org.eclipse.core.runtime.IProgressMonitor;
import org.eclipse.core.runtime.Path;
import org.eclipse.jdt.core.IClasspathEntry;
import org.eclipse.jdt.core.JavaCore;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URL;
import java.net.URLConnection;
import java.util.HashSet;
import java.util.List;

/**
 * A utility class to provide various functionalities for the dependencies. 
 */
public class DependencyManager {
  
  /**
   * Downloads the external dependency jars required for the template defined by 
   * {@link FileTemplateConfig} element and according to the {@link List} of 
   * {@link DependencyConfig} elements. They will be downloaded to the local file 
   * system whose path is defined by {@code extDependencyPath}. 
   * 
   * @param dependencyConfigList the {@link List} of external dependency configurations
   * @param templateConfig the template configuration
   * @param extDependencyPath the output local file system path
   * @param monitor the progress monitor
   * @param noOfTicks the number of ticks allocated
   * @throws CoreException
   *         if anything goes wrong while downloading
   */
  public static void downloadDependencies(List<DependencyConfig> dependencyConfigList, 
      FileTemplateConfig templateConfig, String extDependencyPath, 
          IProgressMonitor monitor, int noOfTicks) throws CoreException {
    for(DependencyConfig dependencyConfig : dependencyConfigList) {
      List<String> dependencyRegexList = templateConfig.getExtDependencies();
      if(dependencyRegexList != null) {
        for(String dependencyRegex : dependencyRegexList) {
          dependencyRegex = ".+" + dependencyRegex.substring(1);
          if(dependencyConfig.getUrl().matches(dependencyRegex)) {
            monitor.subTask("(Downloading " + dependencyConfig.getName() + "...)");
            try {
              download(dependencyConfig.getUrl(),extDependencyPath, monitor, noOfTicks/3);
            } catch(SecurityException e) {
              ExceptionHandler.throwCoreException(e.getMessage(), e);
            } catch(IOException e) {
              ExceptionHandler.throwCoreException(e.getMessage(), e);
            }
          }
        }
      }
    }
  }
  
  /**
   * Returns a {@link HashSet} of all the Continuuity Flow & Data Java Client libraries 
   * required by the template defined by {@link FileTemplateConfig} element found at 
   * path given by {@code javaClientLibPath}. 
   * 
   * @param javaClientLibPath the absolute path on the local file system
   * @param templateConfig the template configuration element
   * @return A {@link HashSet} of all the Continuuity Flow & Data Java Client libraries 
   *         of type {@link IClasspathEntry}
   * @throws CoreException
   *         if anything goes wrong
   */
  public static HashSet<IClasspathEntry> getContinuuityAPIDependencies(String javaClientLibPath, 
      FileTemplateConfig templateConfig) throws CoreException {
    HashSet<IClasspathEntry> entries = new HashSet<IClasspathEntry>();
    try {
      IPath gDataJavaClientLibPath = new Path(javaClientLibPath);
      File gDataJavaClientLibDir = gDataJavaClientLibPath.toFile();
      File[] gDataJavaLibs = gDataJavaClientLibDir.listFiles();
      for(File gDataJavaLib : gDataJavaLibs){
        String gDataJavaLibName = gDataJavaLib.getName().trim();
        List<String> dependencyList = templateConfig.getDependencies();
        for(String dependency : dependencyList) {
          if(gDataJavaLib.isFile() && gDataJavaLibName.matches(dependency)) {
            gDataJavaClientLibPath = new Path(javaClientLibPath + gDataJavaLibName);
            if(entries.add(JavaCore.newLibraryEntry(gDataJavaClientLibPath, null, null))) {
            }
          }
        }
      }
    } catch(NullPointerException e) {
      e.printStackTrace();
      ExceptionHandler.throwCoreException("Null Pointer Exception", e);
    }
        return entries;
  }
  
  /**
   * Returns a {@link HashSet} of all the external dependency libraries 
   * required by the template defined by {@link FileTemplateConfig} element found at 
   * path given by {@code extDependencyPath}.  
   * 
   * @param extDependencyPath the absolute path on the local file system
   * @param templateConfig the template configuration element
   * @return A {@link HashSet} of all the external dependency libraries 
   *         of type {@link IClasspathEntry}
   * @throws CoreException
   *         if anything goes wrong
   */
  public static HashSet<IClasspathEntry> getExtDependencies(
      String extDependencyPath, FileTemplateConfig templateConfig) throws CoreException {
    HashSet<IClasspathEntry> entries = new HashSet<IClasspathEntry>();
    try {
      IPath extLibPath = new Path(extDependencyPath);
      File extLibDir = extLibPath.toFile();
      File[] extLibs = extLibDir.listFiles();
      for(File extLib : extLibs){
        String extLibName = extLib.getName().trim();
        List<String> extDependencyList = templateConfig.getExtDependencies();
        for(String dependency : extDependencyList) {
          if(extLib.isFile() && extLibName.matches(dependency)) {
            extLibPath = new Path(extDependencyPath+extLibName);
            if(entries.add(JavaCore.newLibraryEntry(extLibPath, null, null))) {
            }
          }
        }
      }
    } catch(NullPointerException e) {
      e.printStackTrace();
      ExceptionHandler.throwCoreException("Null Pointer Exception", e);
    }
    
    return entries;
  }
  
  
  /**
   * Performs the actual download
   * @param address the URL of the file to be downloaded
   * @param dirName the directory to which the files are to be downloaded
   * @param monitor the progress monitor
   * @param noOfTicks 
   * @throws SecurityException
   * @throws FileNotFoundException
   * @throws IOException
   */
  private static void download(String address, String dirName, IProgressMonitor monitor, 
      int noOfTicks) throws SecurityException, FileNotFoundException, IOException {
    int noOfTicksLeft; 
    String fileName = dirName + getFileNameFromAddress(address);
    File newDir= new File(dirName);
    File newFile= new File(fileName);
    OutputStream out = null;
    URLConnection connection = null;
    InputStream in = null;
    
    try {
      newDir.mkdir();
      monitor.worked((int) (0.1 * noOfTicks));
      noOfTicksLeft = noOfTicks -(int) (0.1 * noOfTicks); 
      newFile.createNewFile();
      monitor.worked((int) (0.1 * noOfTicks));
      noOfTicksLeft -= (int) (0.1* noOfTicks);
      URL url = new URL(address);
      out = new BufferedOutputStream(new FileOutputStream(fileName));
      connection = url.openConnection();
      in = connection.getInputStream();
      byte[] buffer = new byte[1024];
      int bytesRead;
      float partialWork = 0;
      long bytesWritten = 0;
      while((bytesRead = in.read(buffer)) != -1){
        out.write(buffer, 0, bytesRead);
        bytesWritten += bytesRead;
        partialWork += ((float)bytesRead/connection.getContentLength()) * noOfTicksLeft;
        if(partialWork >= 10){
          monitor.worked((int) partialWork);
          partialWork = 0;
        }
      }
    } catch(SecurityException e) {
      e.printStackTrace();
      throw e;
    } catch(IOException e) {
      e.printStackTrace();
      throw e;
    } finally {
      try {
        if (in != null) {
            in.close();
        }
        if (out != null) {
            out.close();
        }
      } catch (IOException e) {
        e.printStackTrace();
        throw e;
      }
    }
  }

  private static String getFileNameFromAddress(String address) {
    int lastSlashIndex = address.lastIndexOf('/');
    if(lastSlashIndex >= 0 && lastSlashIndex < address.length()-1) {
      return address.substring(lastSlashIndex + 1);
    } else {
      return null;
    }
  }
}
