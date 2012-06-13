package com.continuuity.common.classloader;

import org.junit.Test;

@SuppressWarnings("unused")
public class SimpleClassLoaderTest {

  @Test
  public void testVerySimpleInternalClassLoading() throws Exception {
    
    String a = VerySimpleInternalClass.class.getCanonicalName();
    String b = VerySimpleInternalClass.class.getName();
    String c = VerySimpleInternalClass.class.getSimpleName();
    
    VerySimpleInternalClass vsc =
        (VerySimpleInternalClass)Class.forName(
            "com.continuuity.common.classloader.SimpleClassLoaderTest$VerySimpleInternalClass").newInstance();
    
  }

  @Test
  public void testVerySimpleExternalClassLoading() throws Exception {
    
    String a = VerySimpleExternalClass.class.getCanonicalName();
    String b = VerySimpleExternalClass.class.getName();
    String c = VerySimpleExternalClass.class.getSimpleName();

    VerySimpleExternalClass vsc =
        (VerySimpleExternalClass)Class.forName(
            "com.continuuity.common.classloader.VerySimpleExternalClass").newInstance();
    
  }

  public static class VerySimpleInternalClass {
    
  }
}
