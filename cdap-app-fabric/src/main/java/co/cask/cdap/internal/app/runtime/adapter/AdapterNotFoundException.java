package co.cask.cdap.internal.app.runtime.adapter;

import java.lang.Exception;
import java.lang.String;

/**
 *
 */
public class AdapterNotFoundException extends Exception {


  public AdapterNotFoundException(String message) {
    super(message);
  }
}
