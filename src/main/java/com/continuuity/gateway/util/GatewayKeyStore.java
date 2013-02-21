package com.continuuity.gateway.util;

import java.io.ByteArrayInputStream;
import java.io.InputStream;

/**
 *
 */
public class GatewayKeyStore {

  //TODO:
  //This is dummy for now - replace with real thing!
  public static char[] getKeyStorePassword() {
    return "secret".toCharArray();
  }


  //TODO:
  //This is dummy for now - replace with real thing!
  public static char[] getCertificatePassword() {
    return "secret".toCharArray();
  }

  public static InputStream asInputStream(){
    return new ByteArrayInputStream("sercret".getBytes()) ;

  }

}
