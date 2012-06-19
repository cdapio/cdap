package com.continuuity.common.classloader;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.*;
import java.util.zip.*;

/**
 * JarResources: JarResources maps all resources included in a
 * Zip or Jar file. Additionaly, it provides a method to extract one
 * as a blob.
 */
public final class JarResources {
  private static final Logger LOG = LoggerFactory.getLogger(JarResources.class);

  // jar resource mapping tables
  private Hashtable htSizes=new Hashtable();
  private Hashtable htJarContents=new Hashtable();

  // a jar file
  private String jarFileName;

  /**
   * creates a JarResources. It extracts all resources from a Jar
   * into an internal hashtable, keyed by resource names.
   * @param jarFileName a jar or zip file
   */
  public JarResources(String jarFileName) throws JarResourceException {
    this.jarFileName=jarFileName;
    init();
  }

  /**
   * Extracts a jar resource as a blob.
   * @param name a resource name.
   */
  public byte[] getResource(String name) {
    LOG.debug("Resource name = " + name);
    return (byte[])htJarContents.get(name);
  }

  /**
   * initializes internal hash tables with Jar file resources.
   */
  @SuppressWarnings("unchecked")
  private void init() throws JarResourceException {
    try {
      // extracts just sizes only.
      ZipFile zf=new ZipFile(jarFileName);
      Enumeration e=zf.entries();

      while (e.hasMoreElements()) {
        ZipEntry ze=(ZipEntry)e.nextElement();
        LOG.debug(dumpZipEntry(ze));
        htSizes.put(ze.getName(),new Integer((int)ze.getSize()));
      }
      zf.close();

      // extract resources and put them into the hashtable.
      FileInputStream fis=new FileInputStream(jarFileName);
      BufferedInputStream bis=new BufferedInputStream(fis);
      ZipInputStream zis=new ZipInputStream(bis);
      ZipEntry ze=null;
      while ((ze=zis.getNextEntry())!=null) {
        if (ze.isDirectory()) {
          continue;
        }

        LOG.debug("ze.getName()="+ze.getName()+
              ","+"getSize()="+ze.getSize() );

        int size=(int)ze.getSize();
        // -1 means unknown size.
        if (size==-1){
          size=((Integer)htSizes.get(ze.getName())).intValue();
        }

        byte[] b=new byte[(int)size];
        int rb=0;
        int chunk=0;
        while (((int)size - rb) > 0){
          chunk=zis.read(b,rb,(int)size - rb);
          if (chunk==-1){
            break;
          }
          rb+=chunk;
        }

        // add to internal resource hashtable
        htJarContents.put(ze.getName(),b);
        LOG.debug(ze.getName() + " rb=" + rb + ",size=" + size + ",csize=" + ze.getCompressedSize());
      }
    } catch (NullPointerException e){
      LOG.warn("Error during initialization resource. Reason {}", e.getMessage());
      throw new JarResourceException("Null pointer while loading jar file " + jarFileName);
    } catch (FileNotFoundException e) {
      LOG.warn("File {} not found. Reason : {}", jarFileName, e.getMessage());
      throw new JarResourceException("Jar file " + jarFileName + " requested to be loaded is not found");
    } catch (IOException e) {
      LOG.warn("Error while reading file {}. Reason : {}", jarFileName, e.getMessage());
      throw new JarResourceException("Error reading file " + jarFileName + ".");
    }
  }

  /**
   * Dumps a zip entry into a string.
   * @param ze a ZipEntry
   */
  private String dumpZipEntry(ZipEntry ze) {
    StringBuffer sb=new StringBuffer();
    if (ze.isDirectory()) {
      sb.append("d ");
    } else {
      sb.append("f ");
    }

    if (ze.getMethod()==ZipEntry.STORED) {
      sb.append("stored   ");
    } else {
      sb.append("defalted ");
    }

    sb.append(ze.getName());
    sb.append("\t");
    sb.append(""+ze.getSize());
    if (ze.getMethod()==ZipEntry.DEFLATED) {
      sb.append("/"+ze.getCompressedSize());
    }

    return (sb.toString());
  }

}
