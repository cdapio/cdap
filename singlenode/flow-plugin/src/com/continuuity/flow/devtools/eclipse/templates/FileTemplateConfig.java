package com.continuuity.flow.devtools.eclipse.templates;

import java.util.List;


/**
 * A {@link FileTemplateConfig} object holds all the information pertaining 
 * to a particular template.
 * 
 * <p> Please refer to the
 * <a href="http://gdata-java-client-eclipse-plugin.googlecode.com/svn/trunk/
 *Google%20Data%20Plugin/bin/com/google/gdata/devtools/eclipse/templates.xml">
 * template configuration file</a>.
 */
public class FileTemplateConfig {

  private String name;
  private String fileName;
  private List<String> dependencies;
  private int dependencyCount;
  private boolean isExtDependencyRequired;
  private List<String> extDependencyNames;
  private List<String> extDependencies;
  private String description;
  private String feedUrl;
  private List<String> imports;
  private List<String> args;
  private String serviceClass;
  private String feedClass;
  private String entryClass;
  
  protected FileTemplateConfig() {
    
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
   * @return the fileName
   */
  public String getFileName() {
    return fileName;
  }
  
  /**
   * @param fileName the fileName to set
   */
  void setFileName(String fileName) {
    this.fileName = fileName;
  }
  
  /**
   * @return the dependency
   */
  public List<String> getDependencies() {
    return dependencies;
  }
  
  /**
   * @param dependency the dependency to set
   */
  void setDependencies(List<String> dependency) {
    this.dependencies = dependency;
  }
  
  /**
   * @return the dependencyCount
   */
  public int getDependencyCount() {
    return dependencyCount;
  }
  
  /**
   * @param dependencyCount the dependencyCount to set
   */
  void setDependencyCount(int dependencyCount) {
    this.dependencyCount = dependencyCount;
  }
  
  /**
   * @return the isExtDependencyRequired
   */
  public boolean isExtDependencyRequired() {
    return isExtDependencyRequired;
  }
  
  /**
   * @param isExtDependencyRequired the isExtDependencyRequired to set
   */
  void setExtDependencyRequired(
      boolean isExtDependencyRequired) {
    this.isExtDependencyRequired = isExtDependencyRequired;
  }
  
  /**
   * @return the extDependencyNames
   */
  public List<String> getExtDependencyNames() {
    return extDependencyNames;
  }

  /**
   * @param extDependencyNames the extDependencyNames to set
   */
  void setExtDependencyNames(List<String> extDependencyNames) {
    this.extDependencyNames = extDependencyNames;
  }

  /**
   * @return the extDependencies
   */
  public List<String> getExtDependencies() {
    return extDependencies;
  }

  /**
   * @param extDependencies the extDependencies to set
   */
  void setExtDependencies(List<String> extDependencies) {
    this.extDependencies = extDependencies;
  }

  /**
   * @return the description
   */
  public String getDescription() {
    return description;
  }
  
  /**
   * @param description the description to set
   */
  void setDescription(String description) {
    this.description = description;
  }
  
  /**
   * @return the feedUrl
   */
  public String getFeedUrl() {
    return feedUrl;
  }
  
  /**
   * @param feedUrl the feedUrl to set
   */
  void setFeedUrl(String feedUrl) {
    this.feedUrl = feedUrl;
  }
  
  /**
   * @return the imports
   */
  public List<String> getImports() {
    return imports;
  }
  
  /**
   * @param imports the imports to set
   */
  void setImports(List<String> imports) {
    this.imports = imports;
  }
  
  /**
   * @return the args
   */
  public List<String> getArgs() {
    return args;
  }
  
  /**
   * @param args the args to set
   */
  void setArgs(List<String> args) {
    this.args = args;
  }

  /**
   * @return the serviceClass
   */
  public String getServiceClass() {
    return serviceClass;
  }

  /**
   * @param serviceClass the serviceClass to set
   */
  void setServiceClass(String serviceClass) {
    this.serviceClass = serviceClass;
  }

  /**
   * @return the feedClass
   */
  public String getFeedClass() {
    return feedClass;
  }

  /**
   * @param feedClass the feedClass to set
   */
  void setFeedClass(String feedClass) {
    this.feedClass = feedClass;
  }

  /**
   * @return the entryClass
   */
  public String getEntryClass() {
    return entryClass;
  }

  /**
   * @param entryClass the entryClass to set
   */
  void setEntryClass(String entryClass) {
    this.entryClass = entryClass;
  }
  
}
