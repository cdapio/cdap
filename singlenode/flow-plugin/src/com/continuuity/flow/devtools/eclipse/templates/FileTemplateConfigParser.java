package com.continuuity.flow.devtools.eclipse.templates;

import com.continuuity.flow.devtools.eclipse.Activator;
import com.continuuity.flow.devtools.eclipse.ExceptionHandler;
import org.eclipse.core.runtime.CoreException;
import org.eclipse.core.runtime.FileLocator;
import org.eclipse.core.runtime.Path;
import org.eclipse.core.runtime.Platform;
import org.osgi.framework.Bundle;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;


/**
 * A utility class to parse the 
 * <a href="http://gdata-java-client-eclipse-plugin.googlecode.com/svn/trunk/
 *Google%20Data%20Plugin/bin/com/google/gdata/devtools/eclipse/templates.xml">
 * template configuration file</a> and return a 
 * collection of all the template configuration elements. 
 */
public class FileTemplateConfigParser {
  
  private static final String PARENT_TAG = "TEMPLATE";
  private static final String NAME_ATTRIBUTE = "NAME";
  private static final String ISREQUIRED_ATTRIBUTE = "ISREQUIRED";
  
  private static final String TEMPLATE_XML_FILE = "templates.xml";
  
  private static DocumentBuilderFactory factory;
  private static DocumentBuilder builder;
  private static Document doc;
  
  private static enum Tags {
    FILE_NAME,
    DEPENDENCIES,
    DEPENDENCY_COUNT,
    EXT_DEPENDENCIES,
    DESCRIPTION,
    FEED_URL,
    IMPORTS,
    ARGS
  }
  
  /**
   * This class is non-instantiable.
   */
  private FileTemplateConfigParser() {
    
  }
  
  private static String getTemplateDirectory() throws NullPointerException, IOException {
    String pluginId = Activator.PLUGIN_ID;
    Bundle core = Platform.getBundle(pluginId);
    String templateDir = new String();
    String relativeDir = FileTemplateConfigParser.class.getPackage().getName().replace('.', '/');
    URL imageUrl = FileLocator.find(core, new Path(relativeDir), null);
    if(imageUrl == null) {
      imageUrl = FileLocator.find(core, new Path("src/"+relativeDir), null);
    }
    templateDir = FileLocator.toFileURL(imageUrl).toString();
    templateDir = templateDir.substring("file:".length());
    return templateDir;
  }
  
  
  /**
   * Parses the <a href="http://gdata-java-client-eclipse-plugin.googlecode.com/svn/trunk/
   *Google%20Data%20Plugin/bin/com/google/gdata/devtools/eclipse/templates.xml">
   * template configuration file</a> and returns a {@link Map} of template configuration
   * elements. The {@link Map} has the template name as the key and the configuration element
   * as the object. 
   * 
   * @return  A Map of template configuration elements
   * @throws CoreException
   *         if anything goes wrong 
   */
  public static Map<String, FileTemplateConfig> getConfigElements() throws CoreException {
    Map<String, FileTemplateConfig> templateConfigsMap = 
        new HashMap<String, FileTemplateConfig>();
    factory = DocumentBuilderFactory.newInstance();
    
    try {
      builder = factory.newDocumentBuilder();
      doc = builder.parse(getTemplateDirectory() + TEMPLATE_XML_FILE);
      NodeList templateNodeList = doc.getElementsByTagName(PARENT_TAG);
      for(int i = 0; i < templateNodeList.getLength(); i++) {
        Node template = templateNodeList.item(i);
        NodeList childNodeList = template.getChildNodes();
        FileTemplateConfig templateConfig = new FileTemplateConfig();
        // set Name
        templateConfig.setName(getAttributeValue(template,NAME_ATTRIBUTE));
        for(int j = 1; j < childNodeList.getLength(); j += 2) {
          Node child = childNodeList.item(j);
          switch(Tags.valueOf(child.getNodeName())) {
            case FILE_NAME: 
              templateConfig.setFileName(getTrimedText(child));
              break;
              
            case DEPENDENCIES:
              List<String> dependenciesList = new ArrayList<String>();
              NodeList dependencyChildren = child.getChildNodes();
              for(int k = 1; k < dependencyChildren.getLength(); k += 2) {
                dependenciesList.add(getTrimedText(dependencyChildren.item(k)));
              }
              templateConfig.setDependencies(dependenciesList);
              break;
              
            case DEPENDENCY_COUNT:
              templateConfig.setDependencyCount(Integer.parseInt(getTrimedText(child)));
              break;
              
            case EXT_DEPENDENCIES:
              boolean isRequired = getAttributeValue(child, ISREQUIRED_ATTRIBUTE)
                  .equalsIgnoreCase("true");
              templateConfig.setExtDependencyRequired(isRequired);
              if(isRequired) {
                List<String> extDependencyList = new ArrayList<String>();
                List<String> extDependencyNameList = new ArrayList<String>();
                NodeList thirPartyDependencyChildren = child.getChildNodes();
                for(int k = 1; k < thirPartyDependencyChildren.getLength(); k += 2) {
                  Node extDependencyChild = thirPartyDependencyChildren.item(k);
                  extDependencyNameList.add(getAttributeValue(extDependencyChild,
                      NAME_ATTRIBUTE));
                  extDependencyList.add(getTrimedText(extDependencyChild));
                }
                templateConfig.setExtDependencyNames(extDependencyNameList);
                templateConfig.setExtDependencies(extDependencyList);
              }
              break;
              
            case DESCRIPTION:
              templateConfig.setDescription(getTrimedText(child));
              break;
              
            case FEED_URL:
              templateConfig.setFeedUrl(getTrimedText(child));
              break;
              
            case IMPORTS:
              List<String> importsList = new ArrayList<String>();
              NodeList importChildren = child.getChildNodes();
              for(int k = 1; k < importChildren.getLength(); k += 2) {
                importsList.add(getTrimedText(importChildren.item(k)));
              }
              templateConfig.setImports(importsList);
              break;
              
            case ARGS:
              List<String> argsList = new ArrayList<String>();
              NodeList argChildren = child.getChildNodes();
              for(int k = 1; k < argChildren.getLength(); k += 2) {
                argsList.add(getTrimedText(argChildren.item(k)));
              }
              templateConfig.setArgs(argsList);
              break;
              
            default:
          }
        }
        // Set Service class, Feed class and Entry class names
        List<String> imports = templateConfig.getImports();
        for(int k = 0; k < imports.size(); k++) {
          String singleImport = imports.get(k);
          int lastDotIndex = singleImport.lastIndexOf('.');
          if(singleImport.contains("Service")) {
            templateConfig.setServiceClass(singleImport.substring(lastDotIndex + 1));
          } else if(singleImport.contains("Feed")) {
            templateConfig.setFeedClass(singleImport.substring(lastDotIndex + 1));
          } else if(singleImport.contains("Entry")) {
            templateConfig.setEntryClass(singleImport.substring(lastDotIndex + 1));
          }
        }
        templateConfigsMap.put(templateConfig.getName(), templateConfig);
      }
    } catch(ParserConfigurationException e) {
      e.printStackTrace();
      ExceptionHandler.throwCoreException(e.getMessage(), e);
    } catch(IOException e) {
      e.printStackTrace();
      ExceptionHandler.throwCoreException(e.getMessage(), e);
    } catch(SAXException e) {
      e.printStackTrace();
      ExceptionHandler.throwCoreException(e.getMessage(), e);
    } catch(NumberFormatException e) {
      e.printStackTrace();
      ExceptionHandler.throwCoreException(e.getMessage(), e);
    } catch(NullPointerException e) {
      ExceptionHandler.throwCoreException("Null Pointer Exception", e);
    } catch(Exception e) {
      e.printStackTrace();
      ExceptionHandler.throwCoreException(e.getMessage(), e);
    }
    return new TreeMap<String, FileTemplateConfig>(templateConfigsMap);
  }
  
  private static String getAttributeValue(Node node, String name) {
    return getTrimedText(node.getAttributes().getNamedItem(name));
  }
  
  private static String getTrimedText(Node node) {
    return node.getTextContent().trim();
  }
}
