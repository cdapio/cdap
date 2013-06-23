package com.continuuity.flow.devtools.eclipse;

/**
 * A {@link DependencyConfig} object holds all the information pertaining 
 * to a particular external dependency.
 */
public class DependencyConfig {

  private String name;
  private String url;
  
  protected DependencyConfig() {
    
  }

  /**
   * @return the name
   */
  public String getName() {
    return name;
  }

  /**
   * @param name the name to set
   */
  void setName(String name) {
    this.name = name;
  }

  /**
   * @return the url
   */
  public String getUrl() {
    return url;
  }

  /**
   * @param url the url to set
   */
  void setUrl(String url) {
    this.url = url;
  }
  
}
