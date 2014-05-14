/*
 * Copyright (c) 2012-2013 Continuuity Inc. All rights reserved.
 *
 * The original Apache 2.0 licensed code was changed by Continuuity Inc.
 */

package com.continuuity.common.conf;

import com.google.common.base.Preconditions;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerator;
import org.w3c.dom.Comment;
import org.w3c.dom.DOMException;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.w3c.dom.Text;
import org.xml.sax.SAXException;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.io.Writer;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.WeakHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

/**
 * Provides access to configuration parameters.
 *
 * <h4 id="Resources">Resources</h4>
 *
 * <p>Configurations are specified by resources. A resource contains a set of
 * name/value pairs as XML data. Each resource is named by a <code>String</code>.
 * If named by a <code>String</code>,
 * then the classpath is examined for a file with that name.  If named by a
 * <code>Path</code>, then the local filesystem is examined directly, without
 * referring to the classpath.
 *
 * <p>Unless explicitly turned off, Hadoop by default specifies two
 * resources, loaded in-order from the classpath: <ol>
 * <li><tt><a href="{@docRoot}/../core-default.html">core-default.xml</a>
 * </tt>: Read-only defaults for hadoop.</li>
 * <li><tt>core-site.xml</tt>: Site-specific configuration for a given hadoop
 * installation.</li>
 * </ol>
 * Applications may add additional resources, which are loaded
 * subsequent to these resources in the order they are added.
 *
 * <h4 id="FinalParams">Final Parameters</h4>
 *
 * <p>Configuration parameters may be declared <i>final</i>.
 * Once a resource declares a value final, no subsequently-loaded
 * resource can alter that value.
 * For example, one might define a final parameter with:
 * <tt><pre>
 *  &lt;property&gt;
 *    &lt;name&gt;dfs.client.buffer.dir&lt;/name&gt;
 *    &lt;value&gt;/tmp/hadoop/dfs/client&lt;/value&gt;
 *    <b>&lt;final&gt;true&lt;/final&gt;</b>
 *  &lt;/property&gt;</pre></tt>
 *
 * Administrators typically define parameters as final in
 * <tt>core-site.xml</tt> for values that user applications may not alter.
 *
 * <h4 id="VariableExpansion">Variable Expansion</h4>
 *
 * <p>Value strings are first processed for <i>variable expansion</i>. The
 * available properties are:<ol>
 * <li>Other properties defined in this Configuration; and, if a name is
 * undefined here,</li>
 * <li>Properties in {@link System#getProperties()}.</li>
 * </ol>
 *
 * <p>For example, if a configuration resource contains the following property
 * definitions:
 * <tt><pre>
 *  &lt;property&gt;
 *    &lt;name&gt;basedir&lt;/name&gt;
 *    &lt;value&gt;/user/${<i>user.name</i>}&lt;/value&gt;
 *  &lt;/property&gt;
 *
 *  &lt;property&gt;
 *    &lt;name&gt;tempdir&lt;/name&gt;
 *    &lt;value&gt;${<i>basedir</i>}/tmp&lt;/value&gt;
 *  &lt;/property&gt;</pre></tt>
 *
 * When <tt>conf.get("tempdir")</tt> is called, then <tt>${<i>basedir</i>}</tt>
 * will be resolved to another property in this Configuration, while
 * <tt>${<i>user.name</i>}</tt> would then ordinarily be resolved to the value
 * of the System property with that name.
 */
public class Configuration implements Iterable<Map.Entry<String, String>> {
  private static final Log LOG =
    LogFactory.getLog(Configuration.class);

  private boolean quietmode = true;

  /**
   * List of configuration resources.
   */
  private ArrayList<Object> resources = new ArrayList<Object>();

  /**
   * The value reported as the setting resource when a key is set
   * by code rather than a file resource.
   */
  static final String UNKNOWN_RESOURCE = "Unknown";

  /**
   * List of configuration parameters marked <b>final</b>.
   */
  private Set<String> finalParameters = new HashSet<String>();

  /**
   * Configuration objects.
   */
  private static final WeakHashMap<Configuration, Object> REGISTRY =
    new WeakHashMap<Configuration, Object>();

  private static final Map<ClassLoader, Map<String, Class<?>>>
    CACHE_CLASSES = new WeakHashMap<ClassLoader, Map<String, Class<?>>>();

  /**
   * Sentinel value to store negative cache results in {@link #CACHE_CLASSES}.
   */
  private static final Class<?> NEGATIVE_CACHE_SENTINEL =
    NegativeCacheSentinel.class;

  /**
   * Stores the mapping of key to the resource which modifies or loads
   * the key most recently.
   */
  private HashMap<String, String> updatingResource;

  /**
   * Class to keep the information about the keys which replace the deprecated
   * ones.
   *
   * This class stores the new keys which replace the deprecated keys and also
   * gives a provision to have a custom message for each of the deprecated key
   * that is being replaced. It also provides method to get the appropriate
   * warning message which can be logged whenever the deprecated key is used.
   */
  private static class DeprecatedKeyInfo {
    private String[] newKeys;
    private String customMessage;
    private boolean accessed;
    DeprecatedKeyInfo(String[] newKeys, String customMessage) {
      this.newKeys = newKeys;
      this.customMessage = customMessage;
      accessed = false;
    }

    /**
     * Method to provide the warning message. It gives the custom message if
     * non-null, and default message otherwise.
     * @param key the associated deprecated key.
     * @return message that is to be logged when a deprecated key is used.
     */
    private final String getWarningMessage(String key) {
      String warningMessage;
      if (customMessage == null) {
        StringBuilder message = new StringBuilder(key);
        String deprecatedKeySuffix = " is deprecated. Instead, use ";
        message.append(deprecatedKeySuffix);
        for (int i = 0; i < newKeys.length; i++) {
          message.append(newKeys[i]);
          if (i != newKeys.length - 1) {
            message.append(", ");
          }
        }
        warningMessage = message.toString();
      } else {
        warningMessage = customMessage;
      }
      accessed = true;
      return warningMessage;
    }
  }

  /**
   * Stores the deprecated keys, the new keys which replace the deprecated keys
   * and custom message(if any provided).
   */
  private static Map<String, DeprecatedKeyInfo> deprecatedKeyMap =
    new HashMap<String, DeprecatedKeyInfo>();

  /**
   * Stores a mapping from superseding keys to the keys which they deprecate.
   */
  private static Map<String, String> reverseDeprecatedKeyMap =
    new HashMap<String, String>();

  /**
   * checks whether the given <code>key</code> is deprecated.
   *
   * @param key the parameter which is to be checked for deprecation
   * @return <code>true</code> if the key is deprecated and
   *         <code>false</code> otherwise.
   */
  public static boolean isDeprecated(String key) {
    return deprecatedKeyMap.containsKey(key);
  }

  /**
   * Returns the alternate name for a key if the property name is deprecated
   * or if deprecates a property name.
   *
   * @param name property name.
   * @return alternate name.
   */
  private String[] getAlternateNames(String name) {
    String oldName, altNames[] = null;
    DeprecatedKeyInfo keyInfo = deprecatedKeyMap.get(name);
    if (keyInfo == null) {
      altNames = (reverseDeprecatedKeyMap.get(name) != null) ?
                   new String [] {reverseDeprecatedKeyMap.get(name)} : null;
      if (altNames != null && altNames.length > 0) {
        //To help look for other new configs for this deprecated config
        keyInfo = deprecatedKeyMap.get(altNames[0]);
      }
    }
    if (keyInfo != null && keyInfo.newKeys.length > 0) {
      List<String> list = new ArrayList<String>();
      if (altNames != null) {
        list.addAll(Arrays.asList(altNames));
      }
      list.addAll(Arrays.asList(keyInfo.newKeys));
      altNames = list.toArray(new String[list.size()]);
    }
    return altNames;
  }

  /**
   * Checks for the presence of the property <code>name</code> in the
   * deprecation map. Returns the first of the list of new keys if present
   * in the deprecation map or the <code>name</code> itself. If the property
   * is not presently set but the property map contains an entry for the
   * deprecated key, the value of the deprecated key is set as the value for
   * the provided property name.
   *
   * @param name the property name
   * @return the first property in the list of properties mapping
   *         the <code>name</code> or the <code>name</code> itself.
   */
  private String[] handleDeprecation(String name) {
    ArrayList<String> names = new ArrayList<String>();
    if (isDeprecated(name)) {
      DeprecatedKeyInfo keyInfo = deprecatedKeyMap.get(name);
      warnOnceIfDeprecated(name);
      for (String newKey : keyInfo.newKeys) {
        if (newKey != null) {
          names.add(newKey);
        }
      }
    }
    if (names.isEmpty()) {
      names.add(name);
    }
    for (String n : names) {
      String deprecatedKey = reverseDeprecatedKeyMap.get(n);
      if (deprecatedKey != null && !getOverlay().containsKey(n) &&
            getOverlay().containsKey(deprecatedKey)) {
        getProps().setProperty(n, getOverlay().getProperty(deprecatedKey));
        getOverlay().setProperty(n, getOverlay().getProperty(deprecatedKey));
      }
    }
    return names.toArray(new String[names.size()]);
  }

  private void handleDeprecation() {
    LOG.trace("Handling deprecation for all properties in config...");
    Set<Object> keys = new HashSet<Object>();
    keys.addAll(getProps().keySet());
    for (Object item: keys) {
      LOG.trace("Handling deprecation for " + item);
      handleDeprecation((String) item);
    }
  }

  private Properties properties;
  private Properties overlay;
  private ClassLoader classLoader;

  {
    classLoader = Thread.currentThread().getContextClassLoader();
    if (classLoader == null) {
      classLoader = Configuration.class.getClassLoader();
    }
  }

  /** A new configuration. */
  public Configuration() {
    updatingResource = new HashMap<String, String>();
    synchronized (Configuration.class) {
      REGISTRY.put(this, null);
    }
  }

  /**
   * A new configuration with the same settings cloned from another.
   *
   * @param other the configuration from which to clone settings.
   */
  @SuppressWarnings("unchecked")
  public Configuration(Configuration other) {
    this.resources = (ArrayList) other.resources.clone();
    synchronized (other) {
      if (other.properties != null) {
        this.properties = (Properties) other.properties.clone();
      }

      if (other.overlay != null) {
        this.overlay = (Properties) other.overlay.clone();
      }

      this.updatingResource = new HashMap<String, String>(other.updatingResource);
    }

    this.finalParameters = new HashSet<String>(other.finalParameters);
    synchronized (Configuration.class) {
      REGISTRY.put(this, null);
    }
    this.classLoader = other.classLoader;
    setQuietMode(other.getQuietMode());
  }

  /**
   * Add a configuration resource.
   *
   * The properties of this resource will override properties of previously
   * added resources, unless they were marked <a href="#Final">final</a>.
   *
   * @param name resource to be added, the classpath is examined for a file
   *             with that name.
   */
  public void addResource(String name) {
    addResourceObject(name);
  }

  /**
   * Add a configuration resource.
   *
   * The properties of this resource will override properties of previously
   * added resources, unless they were marked <a href="#Final">final</a>.
   *
   * @param url url of the resource to be added, the local filesystem is
   *            examined directly to find the resource, without referring to
   *            the classpath.
   */
  public void addResource(URL url) {
    addResourceObject(url);
  }

  /**
   * Add a configuration resource.
   *
   * The properties of this resource will override properties of previously
   * added resources, unless they were marked <a href="#Final">final</a>.
   *
   * @param in InputStream to deserialize the object from.
   */
  public void addResource(InputStream in) {
    addResourceObject(in);
  }

  /**
   * Reload configuration from previously added resources.
   *
   * This method will clear all the configuration read from the added
   * resources, and final parameters. This will make the resources to
   * be read again before accessing the values. Values that are added
   * via set methods will overlay values read from the resources.
   */
  public synchronized void reloadConfiguration() {
    properties = null;                            // trigger reload
    finalParameters.clear();                      // clear site-limits
  }

  private synchronized void addResourceObject(Object resource) {
    resources.add(resource);                      // add to resources
    reloadConfiguration();
  }

  private static final Pattern VAR_PAT = Pattern.compile("\\$\\{[^\\}\\$\u0020]+\\}");
  private static final int MAX_SUBST = 20;

  private String substituteVars(String expr) {
    if (expr == null) {
      return null;
    }
    Matcher match = VAR_PAT.matcher("");
    String eval = expr;
    for (int s = 0; s < MAX_SUBST; s++) {
      match.reset(eval);
      if (!match.find()) {
        return eval;
      }
      String var = match.group();
      var = var.substring(2, var.length() - 1); // remove ${ .. }
      String val = null;
      try {
        val = System.getProperty(var);
      } catch (SecurityException se) {
        LOG.warn("Unexpected SecurityException in Configuration", se);
      }
      if (val == null) {
        val = getRaw(var);
      }
      if (val == null) {
        return eval; // return literal ${var}: var is unbound
      }
      // substitute
      eval = eval.substring(0, match.start()) + val + eval.substring(match.end());
    }
    throw new IllegalStateException("Variable substitution depth too large: "
                                      + MAX_SUBST + " " + expr);
  }

  /**
   * Get the value of the <code>name</code> property, <code>null</code> if
   * no such property exists. If the key is deprecated, it returns the value of
   * the first key which replaces the deprecated key and is not null
   *
   * Values are processed for <a href="#VariableExpansion">variable expansion</a>
   * before being returned.
   *
   * @param name the property name.
   * @return the value of the <code>name</code> or its replacing property,
   *         or null if no such property exists.
   */
  public String get(String name) {
    String[] names = handleDeprecation(name);
    String result = null;
    for (String n : names) {
      result = substituteVars(getProps().getProperty(n));
    }
    return result;
  }

  /**
   * Get the value of the <code>name</code> property as a trimmed <code>String</code>,
   * <code>null</code> if no such property exists.
   * If the key is deprecated, it returns the value of
   * the first key which replaces the deprecated key and is not null
   *
   * Values are processed for <a href="#VariableExpansion">variable expansion</a>
   * before being returned.
   *
   * @param name the property name.
   * @return the value of the <code>name</code> or its replacing property,
   *         or null if no such property exists.
   */
  public String getTrimmed(String name) {
    String value = get(name);

    if (null == value) {
      return null;
    } else {
      return value.trim();
    }
  }

  /**
   * Get the value of the <code>name</code> property, without doing
   * <a href="#VariableExpansion">variable expansion</a>.If the key is
   * deprecated, it returns the value of the first key which replaces
   * the deprecated key and is not null.
   *
   * @param name the property name.
   * @return the value of the <code>name</code> property or
   *         its replacing property and null if no such property exists.
   */
  public String getRaw(String name) {
    String[] names = handleDeprecation(name);
    String result = null;
    for (String n : names) {
      result = getProps().getProperty(n);
    }
    return result;
  }

  /**
   * Set the <code>value</code> of the <code>name</code> property. If
   * <code>name</code> is deprecated or there is a deprecated name associated to it,
   * it sets the value to both names.
   *
   * @param name property name.
   * @param value property value.
   */
  public void set(String name, String value) {
    if (deprecatedKeyMap.isEmpty()) {
      getProps();
    }
    getOverlay().setProperty(name, value);
    getProps().setProperty(name, value);
    updatingResource.put(name, UNKNOWN_RESOURCE);
    String[] altNames = getAlternateNames(name);
    if (altNames != null && altNames.length > 0) {
      for (String altName : altNames) {
        getOverlay().setProperty(altName, value);
        getProps().setProperty(altName, value);
      }
    }
    warnOnceIfDeprecated(name);
  }

  private void warnOnceIfDeprecated(String name) {
    DeprecatedKeyInfo keyInfo = deprecatedKeyMap.get(name);
    if (keyInfo != null && !keyInfo.accessed) {
      LOG.warn(keyInfo.getWarningMessage(name));
    }
  }

  /**
   * Unset a previously set property.
   */
  public synchronized void unset(String name) {
    String[] altNames = getAlternateNames(name);
    getOverlay().remove(name);
    getProps().remove(name);
    if (altNames != null && altNames.length > 0) {
      for (String altName : altNames) {
        getOverlay().remove(altName);
        getProps().remove(altName);
      }
    }
  }

  /**
   * Sets a property if it is currently unset.
   * @param name the property name
   * @param value the new value
   */
  public synchronized void setIfUnset(String name, String value) {
    if (get(name) == null) {
      set(name, value);
    }
  }

  private synchronized Properties getOverlay() {
    if (overlay == null) {
      overlay = new Properties();
    }
    return overlay;
  }

  /**
   * Get the value of the <code>name</code>. If the key is deprecated,
   * it returns the value of the first key which replaces the deprecated key
   * and is not null.
   * If no such property exists,
   * then <code>defaultValue</code> is returned.
   *
   * @param name property name.
   * @param defaultValue default value.
   * @return property value, or <code>defaultValue</code> if the property
   *         doesn't exist.
   */
  public String get(String name, String defaultValue) {
    String[] names = handleDeprecation(name);
    String result = null;
    for (String n : names) {
      result = substituteVars(getProps().getProperty(n, defaultValue));
    }
    return result;
  }

  /**
   * Get the value of the {@code name} configuration property as an {@code int}.  If the property is missing
   * from the configuration or is not a valid {@code int}, then an exception is thrown.
   *
   * @param name the configuration property name
   * @throws NumberFormatException if the configured value is not a valid {@code int}
   * @throws NullPointerException if the configuration property is not present in the loaded config
   * @return the configuration property value as an {@code int}
   */
  public int getInt(String name) {
    String valueString = getTrimmed(name);
    Preconditions.checkNotNull(valueString);
    String hexString = getHexDigits(valueString);
    if (hexString != null) {
      return Integer.parseInt(hexString, 16);
    }
    return Integer.parseInt(valueString);
  }

  /**
   * Get the value of the <code>name</code> property as an <code>int</code>.
   *
   * If no such property exists, the provided default value is returned,
   * or if the specified value is not a valid <code>int</code>,
   * then an error is thrown.
   *
   * @param name property name.
   * @param defaultValue default value.
   * @throws NumberFormatException when the value is invalid
   * @return property value as an <code>int</code>,
   *         or <code>defaultValue</code>.
   */
  public int getInt(String name, int defaultValue) {
    String valueString = getTrimmed(name);
    if (valueString == null) {
      return defaultValue;
    }
    String hexString = getHexDigits(valueString);
    if (hexString != null) {
      return Integer.parseInt(hexString, 16);
    }
    return Integer.parseInt(valueString);
  }

  /**
   * Set the value of the <code>name</code> property to an <code>int</code>.
   *
   * @param name property name.
   * @param value <code>int</code> value of the property.
   */
  public void setInt(String name, int value) {
    set(name, Integer.toString(value));
  }

  /**
   * Get the value of the {@code name} configuration property as a {@code long}.  If the config property does
   * does not exist or is not a valid {@code long}, then an exception is thrown.
   *
   * @param name the configuration property name
   * @throws NumberFormatException if the configured value is not a valid {@code long}
   * @throws NullPointerException if the configuration property is not present in the loaded config
   * @return the configuration property value as a {@code long}
   */
  public long getLong(String name) {
    String valueString = getTrimmed(name);
    Preconditions.checkNotNull(valueString);
    String hexString = getHexDigits(valueString);
    if (hexString != null) {
      return Long.parseLong(hexString, 16);
    }
    return Long.parseLong(valueString);
  }

  /**
   * Get the value of the <code>name</code> property as a <code>long</code>.
   * If no such property exists, the provided default value is returned,
   * or if the specified value is not a valid <code>long</code>,
   * then an error is thrown.
   *
   * @param name property name.
   * @param defaultValue default value.
   * @throws NumberFormatException when the value is invalid
   * @return property value as a <code>long</code>,
   *         or <code>defaultValue</code>.
   */
  public long getLong(String name, long defaultValue) {
    String valueString = getTrimmed(name);
    if (valueString == null) {
      return defaultValue;
    }
    String hexString = getHexDigits(valueString);
    if (hexString != null) {
      return Long.parseLong(hexString, 16);
    }
    return Long.parseLong(valueString);
  }

  /**
   * Get the value of the <code>name</code> property as a <code>long</code> or
   * human readable format. If no such property exists or if the specified value is not a valid
   * <code>long</code> or human readable format, then an error is thrown. You
   * can use the following suffix (case insensitive): k(kilo), m(mega), g(giga),
   * t(tera), p(peta), e(exa)
   *
   * @param name property name.
   * @throws NumberFormatException when the value is invalid
   * @throws NullPointerException if the configuration property does not exist
   * @return property value as a <code>long</code>
   */
  public long getLongBytes(String name) {
    String valueString = getTrimmed(name);
    Preconditions.checkNotNull(valueString);
    return StringUtils.TraditionalBinaryPrefix.string2long(valueString);
  }

  /**
   * Get the value of the <code>name</code> property as a <code>long</code> or
   * human readable format. If no such property exists, the provided default
   * value is returned, or if the specified value is not a valid
   * <code>long</code> or human readable format, then an error is thrown. You
   * can use the following suffix (case insensitive): k(kilo), m(mega), g(giga),
   * t(tera), p(peta), e(exa)
   *
   * @param name property name.
   * @param defaultValue default value.
   * @throws NumberFormatException when the value is invalid
   * @return property value as a <code>long</code>,
   *         or <code>defaultValue</code>.
   */
  public long getLongBytes(String name, long defaultValue) {
    String valueString = getTrimmed(name);
    if (valueString == null) {
      return defaultValue;
    }
    return StringUtils.TraditionalBinaryPrefix.string2long(valueString);
  }

  private String getHexDigits(String value) {
    boolean negative = false;
    String str = value;
    String hexString = null;
    if (value.startsWith("-")) {
      negative = true;
      str = value.substring(1);
    }
    if (str.startsWith("0x") || str.startsWith("0X")) {
      hexString = str.substring(2);
      if (negative) {
        hexString = "-" + hexString;
      }
      return hexString;
    }
    return null;
  }

  /**
   * Set the value of the <code>name</code> property to a <code>long</code>.
   *
   * @param name property name.
   * @param value <code>long</code> value of the property.
   */
  public void setLong(String name, long value) {
    set(name, Long.toString(value));
  }

  /**
   * Get the value of the <code>name</code> property as a <code>float</code>.
   * If no such property exists or if the specified value is not a valid <code>float</code>,
   * then an error is thrown.
   *
   * @param name property name.
   * @throws NumberFormatException when the value is invalid
   * @throws NullPointerException if the configuration property does not exist
   * @return property value as a <code>float</code>
   */
  public float getFloat(String name) {
    String valueString = getTrimmed(name);
    Preconditions.checkNotNull(valueString);
    return Float.parseFloat(valueString);
  }

  /**
   * Get the value of the <code>name</code> property as a <code>float</code>.
   * If no such property exists, the provided default value is returned,
   * or if the specified value is not a valid <code>float</code>,
   * then an error is thrown.
   *
   * @param name property name.
   * @param defaultValue default value.
   * @throws NumberFormatException when the value is invalid
   * @return property value as a <code>float</code>,
   *         or <code>defaultValue</code>.
   */
  public float getFloat(String name, float defaultValue) {
    String valueString = getTrimmed(name);
    if (valueString == null) {
      return defaultValue;
    }
    return Float.parseFloat(valueString);
  }
  /**
   * Set the value of the <code>name</code> property to a <code>float</code>.
   *
   * @param name property name.
   * @param value property value.
   */
  public void setFloat(String name, float value) {
    set(name, Float.toString(value));
  }

  /**
   * Get the value of the <code>name</code> property as a <code>boolean</code>.
   * If no such property is specified, or if the specified value is not a valid
   * <code>boolean</code>, then an exception is thrown.
   *
   * @param name property name.
   * @throws NullPointerException if the configuration property does not exist
   * @throws IllegalArgumentException if the configured value is not a valid {@code boolean}
   * @return property value as a <code>boolean</code>
   */
  public boolean getBoolean(String name) {
    String valueString = getTrimmed(name);
    Preconditions.checkNotNull(valueString);

    valueString = valueString.toLowerCase();

    if ("true".equals(valueString)) {
      return true;
    } else if ("false".equals(valueString)) {
      return false;
    }
    throw new IllegalArgumentException("Configured property is not a valid boolean: name="
                                         + name + ", value=" + valueString);
  }

  /**
   * Get the value of the <code>name</code> property as a <code>boolean</code>.
   * If no such property is specified, or if the specified value is not a valid
   * <code>boolean</code>, then <code>defaultValue</code> is returned.
   *
   * @param name property name.
   * @param defaultValue default value.
   * @return property value as a <code>boolean</code>,
   *         or <code>defaultValue</code>.
   */
  public boolean getBoolean(String name, boolean defaultValue) {
    String valueString = getTrimmed(name);
    if (null == valueString || "".equals(valueString)) {
      return defaultValue;
    }

    valueString = valueString.toLowerCase();

    if ("true".equals(valueString)) {
      return true;
    } else if ("false".equals(valueString)) {
      return false;
    } else {
      return defaultValue;
    }
  }

  /**
   * Set the value of the <code>name</code> property to a <code>boolean</code>.
   *
   * @param name property name.
   * @param value <code>boolean</code> value of the property.
   */
  public void setBoolean(String name, boolean value) {
    set(name, Boolean.toString(value));
  }

  /**
   * Set the given property, if it is currently unset.
   * @param name property name
   * @param value new value
   */
  public void setBooleanIfUnset(String name, boolean value) {
    setIfUnset(name, Boolean.toString(value));
  }

  /**
   * Set the value of the <code>name</code> property to the given type. This
   * is equivalent to <code>set(&lt;name&gt;, value.toString())</code>.
   * @param name property name
   * @param value new value
   */
  public <T extends Enum<T>> void setEnum(String name, T value) {
    set(name, value.toString());
  }

  /**
   * Return value matching this enumerated type.
   * @param name Property name
   * @throws NullPointerException if the configuration property does not exist
   * @throws IllegalArgumentException If mapping is illegal for the type
   * provided
   */
  public <T extends Enum<T>> T getEnum(String name, Class<T> declaringClass) {
    final String val = get(name);
    Preconditions.checkNotNull(val);
    return Enum.valueOf(declaringClass, val);
  }

  /**
   * Return value matching this enumerated type.
   * @param name Property name
   * @param defaultValue Value returned if no mapping exists
   * @throws IllegalArgumentException If mapping is illegal for the type
   * provided
   */
  public <T extends Enum<T>> T getEnum(String name, T defaultValue) {
    final String val = get(name);
    return null == val
             ? defaultValue
             : Enum.valueOf(defaultValue.getDeclaringClass(), val);
  }

  /**
   * Get the value of the <code>name</code> property as a <code>Pattern</code>.
   * If no such property is specified, or if the specified value is not a valid
   * <code>Pattern</code>, then an exception is thrown.
   *
   * @param name property name
   * @throws NullPointerException if the configuration property does not exist
   * @throws PatternSyntaxException if the configured value is not a valid {@code Pattern}
   * @return property value as a compiled Pattern
   */
  public Pattern getPattern(String name) {
    String valString = get(name);
    Preconditions.checkNotNull(valString);
    return Pattern.compile(valString);
  }

  /**
   * Get the value of the <code>name</code> property as a <code>Pattern</code>.
   * If no such property is specified, or if the specified value is not a valid
   * <code>Pattern</code>, then <code>DefaultValue</code> is returned.
   *
   * @param name property name
   * @param defaultValue default value
   * @return property value as a compiled Pattern, or defaultValue
   */
  public Pattern getPattern(String name, Pattern defaultValue) {
    String valString = get(name);
    if (null == valString || "".equals(valString)) {
      return defaultValue;
    }
    try {
      return Pattern.compile(valString);
    } catch (PatternSyntaxException pse) {
      LOG.warn("Regular expression '" + valString + "' for property '" +
                 name + "' not valid. Using default", pse);
      return defaultValue;
    }
  }

  /**
   * Set the given property to <code>Pattern</code>.
   * If the pattern is passed as null, sets the empty pattern which results in
   * further calls to getPattern(...) returning the default value.
   *
   * @param name property name
   * @param pattern new value
   */
  public void setPattern(String name, Pattern pattern) {
    if (null == pattern) {
      set(name, null);
    } else {
      set(name, pattern.pattern());
    }
  }

  /**
   * A class that represents a set of positive integer ranges. It parses
   * strings of the form: "2-3,5,7-" where ranges are separated by comma and
   * the lower/upper bounds are separated by dash. Either the lower or upper
   * bound may be omitted meaning all values up to or over. So the string
   * above means 2, 3, 5, and 7, 8, 9, ...
   */
  public static class IntegerRanges implements Iterable<Integer> {
    private static class Range {
      int start;
      int end;
    }

    private static class RangeNumberIterator implements Iterator<Integer> {
      Iterator<Range> internal;
      int at;
      int end;

      public RangeNumberIterator(List<Range> ranges) {
        if (ranges != null) {
          internal = ranges.iterator();
        }
        at = -1;
        end = -2;
      }

      @Override
      public boolean hasNext() {
        if (at <= end) {
          return true;
        } else if (internal != null) {
          return internal.hasNext();
        }
        return false;
      }

      @Override
      public Integer next() {
        if (at <= end) {
          at++;
          return at - 1;
        } else if (internal != null) {
          Range found = internal.next();
          if (found != null) {
            at = found.start;
            end = found.end;
            at++;
            return at - 1;
          }
        }
        return null;
      }

      @Override
      public void remove() {
        throw new UnsupportedOperationException();
      }
    };

    List<Range> ranges = new ArrayList<Range>();

    public IntegerRanges() {
    }

    public IntegerRanges(String newValue) {
      StringTokenizer itr = new StringTokenizer(newValue, ",");
      while (itr.hasMoreTokens()) {
        String rng = itr.nextToken().trim();
        String[] parts = rng.split("-", 3);
        if (parts.length < 1 || parts.length > 2) {
          throw new IllegalArgumentException("integer range badly formed: " +
                                               rng);
        }
        Range r = new Range();
        r.start = convertToInt(parts[0], 0);
        if (parts.length == 2) {
          r.end = convertToInt(parts[1], Integer.MAX_VALUE);
        } else {
          r.end = r.start;
        }
        if (r.start > r.end) {
          throw new IllegalArgumentException("IntegerRange from " + r.start +
                                               " to " + r.end + " is invalid");
        }
        ranges.add(r);
      }
    }

    /**
     * Convert a string to an int treating empty strings as the default value.
     * @param value the string value
     * @param defaultValue the value for if the string is empty
     * @return the desired integer
     */
    private static int convertToInt(String value, int defaultValue) {
      String trim = value.trim();
      if (trim.length() == 0) {
        return defaultValue;
      }
      return Integer.parseInt(trim);
    }

    /**
     * Is the given value in the set of ranges.
     * @param value the value to check
     * @return is the value in the ranges?
     */
    public boolean isIncluded(int value) {
      for (Range r: ranges) {
        if (r.start <= value && value <= r.end) {
          return true;
        }
      }
      return false;
    }

    /**
     * @return true if there are no values in this range, else false.
     */
    public boolean isEmpty() {
      return ranges == null || ranges.isEmpty();
    }

    @Override
    public String toString() {
      StringBuilder result = new StringBuilder();
      boolean first = true;
      for (Range r: ranges) {
        if (first) {
          first = false;
        } else {
          result.append(',');
        }
        result.append(r.start);
        result.append('-');
        result.append(r.end);
      }
      return result.toString();
    }

    @Override
    public Iterator<Integer> iterator() {
      return new RangeNumberIterator(ranges);
    }

  }

  /**
   * Parse the given attribute as a set of integer ranges.
   * @param name the attribute name
   * @throws NullPointerException if the configuration property does not exist
   * @return a new set of ranges from the configured value
   */
  public IntegerRanges getRange(String name) {
    String valueString = get(name);
    Preconditions.checkNotNull(valueString);
    return new IntegerRanges(valueString);
  }

  /**
   * Parse the given attribute as a set of integer ranges.
   * @param name the attribute name
   * @param defaultValue the default value if it is not set
   * @return a new set of ranges from the configured value
   */
  public IntegerRanges getRange(String name, String defaultValue) {
    return new IntegerRanges(get(name, defaultValue));
  }

  /**
   * Get the comma delimited values of the <code>name</code> property as
   * a collection of <code>String</code>s.
   * If no such property is specified then empty collection is returned.
   * <p>
   * This is an optimized version of {@link #getStrings(String)}
   *
   * @param name property name.
   * @return property value as a collection of <code>String</code>s.
   */
  public Collection<String> getStringCollection(String name) {
    String valueString = get(name);
    return StringUtils.getStringCollection(valueString);
  }

  /**
   * Get the comma delimited values of the <code>name</code> property as
   * an array of <code>String</code>s.
   * If no such property is specified then <code>null</code> is returned.
   *
   * @param name property name.
   * @return property value as an array of <code>String</code>s,
   *         or <code>null</code>.
   */
  public String[] getStrings(String name) {
    String valueString = get(name);
    return StringUtils.getStrings(valueString);
  }

  /**
   * Get the comma delimited values of the <code>name</code> property as
   * an array of <code>String</code>s.
   * If no such property is specified then default value is returned.
   *
   * @param name property name.
   * @param defaultValue The default value
   * @return property value as an array of <code>String</code>s,
   *         or default value.
   */
  public String[] getStrings(String name, String... defaultValue) {
    String valueString = get(name);
    if (valueString == null) {
      return defaultValue;
    } else {
      return StringUtils.getStrings(valueString);
    }
  }

  /**
   * Get the comma delimited values of the <code>name</code> property as
   * a collection of <code>String</code>s, trimmed of the leading and trailing whitespace.
   * If no such property is specified then empty <code>Collection</code> is returned.
   *
   * @param name property name.
   * @return property value as a collection of <code>String</code>s, or empty <code>Collection</code>
   */
  public Collection<String> getTrimmedStringCollection(String name) {
    String valueString = get(name);
    if (null == valueString) {
      Collection<String> empty = new ArrayList<String>();
      return empty;
    }
    return StringUtils.getTrimmedStringCollection(valueString);
  }

  /**
   * Get the comma delimited values of the <code>name</code> property as
   * an array of <code>String</code>s, trimmed of the leading and trailing whitespace.
   * If no such property is specified then an empty array is returned.
   *
   * @param name property name.
   * @return property value as an array of trimmed <code>String</code>s,
   *         or empty array.
   */
  public String[] getTrimmedStrings(String name) {
    String valueString = get(name);
    return StringUtils.getTrimmedStrings(valueString);
  }

  /**
   * Get the comma delimited values of the <code>name</code> property as
   * an array of <code>String</code>s, trimmed of the leading and trailing whitespace.
   * If no such property is specified then default value is returned.
   *
   * @param name property name.
   * @param defaultValue The default value
   * @return property value as an array of trimmed <code>String</code>s,
   *         or default value.
   */
  public String[] getTrimmedStrings(String name, String... defaultValue) {
    String valueString = get(name);
    if (null == valueString) {
      return defaultValue;
    } else {
      return StringUtils.getTrimmedStrings(valueString);
    }
  }

  /**
   * Set the array of string values for the <code>name</code> property as
   * as comma delimited values.
   *
   * @param name property name.
   * @param values The values
   */
  public void setStrings(String name, String... values) {
    set(name, StringUtils.arrayToString(values));
  }

  /**
   * Load a class by name.
   *
   * @param name the class name.
   * @return the class object.
   * @throws ClassNotFoundException if the class is not found.
   */
  public Class<?> getClassByName(String name) throws ClassNotFoundException {
    Class<?> ret = getClassByNameOrNull(name);
    if (ret == null) {
      throw new ClassNotFoundException("Class " + name + " not found");
    }
    return ret;
  }

  /**
   * Load a class by name, returning null rather than throwing an exception
   * if it couldn't be loaded. This is to avoid the overhead of creating
   * an exception.
   *
   * @param name the class name
   * @return the class object, or null if it could not be found.
   */
  public Class<?> getClassByNameOrNull(String name) {
    Map<String, Class<?>> map;

    synchronized (CACHE_CLASSES) {
      map = CACHE_CLASSES.get(classLoader);
      if (map == null) {
        map = Collections.synchronizedMap(
                                           new WeakHashMap<String, Class<?>>());
        CACHE_CLASSES.put(classLoader, map);
      }
    }

    Class<?> clazz = map.get(name);
    if (clazz == null) {
      try {
        clazz = Class.forName(name, true, classLoader);
      } catch (ClassNotFoundException e) {
        // Leave a marker that the class isn't found
        map.put(name, NEGATIVE_CACHE_SENTINEL);
        return null;
      }
      // two putters can race here, but they'll put the same class
      map.put(name, clazz);
      return clazz;
    } else if (clazz == NEGATIVE_CACHE_SENTINEL) {
      return null; // not found
    } else {
      // cache hit
      return clazz;
    }
  }

  /**
   * Get the value of the <code>name</code> property
   * as an array of <code>Class</code>.
   * The value of the property specifies a list of comma separated class names.
   * If no such property is specified, then <code>defaultValue</code> is
   * returned.
   *
   * @param name the property name.
   * @param defaultValue default value.
   * @return property value as a <code>Class[]</code>,
   *         or <code>defaultValue</code>.
   */
  public Class<?>[] getClasses(String name, Class<?> ... defaultValue) {
    String[] classnames = getTrimmedStrings(name);
    if (classnames == null) {
      return defaultValue;
    }
    try {
      Class<?>[] classes = new Class<?>[classnames.length];
      for (int i = 0; i < classnames.length; i++) {
        classes[i] = getClassByName(classnames[i]);
      }
      return classes;
    } catch (ClassNotFoundException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Get the value of the <code>name</code> property as a <code>Class</code>.
   * If no such property is specified, then <code>defaultValue</code> is
   * returned.
   *
   * @param name the class name.
   * @param defaultValue default value.
   * @return property value as a <code>Class</code>,
   *         or <code>defaultValue</code>.
   */
  public Class<?> getClass(String name, Class<?> defaultValue) {
    String valueString = getTrimmed(name);
    if (valueString == null) {
      return defaultValue;
    }
    try {
      return getClassByName(valueString);
    } catch (ClassNotFoundException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Get the value of the <code>name</code> property as a <code>Class</code>
   * implementing the interface specified by <code>xface</code>.
   *
   * If no such property is specified, then <code>defaultValue</code> is
   * returned.
   *
   * An exception is thrown if the returned class does not implement the named
   * interface.
   *
   * @param name the class name.
   * @param defaultValue default value.
   * @param xface the interface implemented by the named class.
   * @return property value as a <code>Class</code>,
   *         or <code>defaultValue</code>.
   */
  public <U> Class<? extends U> getClass(String name,
                                         Class<? extends U> defaultValue,
                                         Class<U> xface) {
    try {
      Class<?> theClass = getClass(name, defaultValue);
      if (theClass != null && !xface.isAssignableFrom(theClass)) {
        throw new RuntimeException(theClass + " not " + xface.getName());
      } else if (theClass != null) {
        return theClass.asSubclass(xface);
      } else {
        return null;
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Get the value of the <code>name</code> property as a <code>List</code>
   * of objects implementing the interface specified by <code>xface</code>.
   *
   * An exception is thrown if any of the classes does not exist, or if it does
   * not implement the named interface.
   *
   * @param name the property name.
   * @param xface the interface implemented by the classes named by
   *        <code>name</code>.
   * @return a <code>List</code> of objects implementing <code>xface</code>.
   */
  @SuppressWarnings("unchecked")
  public <U> List<U> getInstances(String name, Class<U> xface) {
    List<U> ret = new ArrayList<U>();
    Class<?>[] classes = getClasses(name);
    for (Class<?> cl: classes) {
      if (!xface.isAssignableFrom(cl)) {
        throw new RuntimeException(cl + " does not implement " + xface);
      }
      ret.add((U) ReflectionUtils.newInstance(cl, this));
    }
    return ret;
  }


  /**
   * Set the value of the <code>name</code> property to the name of a
   * <code>theClass</code> implementing the given interface <code>xface</code>.
   *
   * An exception is thrown if <code>theClass</code> does not implement the
   * interface <code>xface</code>.
   *
   * @param name property name.
   * @param theClass property value.
   * @param xface the interface implemented by the named class.
   */
  public void setClass(String name, Class<?> theClass, Class<?> xface) {
    if (!xface.isAssignableFrom(theClass)) {
      throw new RuntimeException(theClass + " not " + xface.getName());
    }
    set(name, theClass.getName());
  }

  /**
   * Get a local file name under a directory named in <i>dirsProp</i> with
   * the given <i>path</i>.  If <i>dirsProp</i> contains multiple directories,
   * then one is chosen based on <i>path</i>'s hash code.  If the selected
   * directory does not exist, an attempt is made to create it.
   *
   * @param dirsProp directory in which to locate the file.
   * @param path file-path.
   * @return local file under the directory with the given path.
   */
  public File getFile(String dirsProp, String path)
    throws IOException {
    String[] dirs = getTrimmedStrings(dirsProp);
    int hashCode = path.hashCode();
    for (int i = 0; i < dirs.length; i++) {  // try each local dir
      int index = (hashCode + i & Integer.MAX_VALUE) % dirs.length;
      File file = new File(dirs[index], path);
      File dir = file.getParentFile();
      if (dir.exists() || dir.mkdirs()) {
        return file;
      }
    }
    throw new IOException("No valid local directories in property: " + dirsProp);
  }

  /**
   * Get the {@link URL} for the named resource.
   *
   * @param name resource name.
   * @return the url for the named resource.
   */
  public URL getResource(String name) {
    return classLoader.getResource(name);
  }

  /**
   * Get an input stream attached to the configuration resource with the
   * given <code>name</code>.
   *
   * @param name configuration resource name.
   * @return an input stream attached to the resource.
   */
  public InputStream getConfResourceAsInputStream(String name) {
    try {
      URL url = getResource(name);

      if (url == null) {
        LOG.info(name + " not found");
        return null;
      } else {
        LOG.info("found resource " + name + " at " + url);
      }

      return url.openStream();
    } catch (Exception e) {
      return null;
    }
  }

  /**
   * Get a {@link java.io.Reader} attached to the configuration resource with the
   * given <code>name</code>.
   *
   * @param name configuration resource name.
   * @return a reader attached to the resource.
   */
  public Reader getConfResourceAsReader(String name) {
    try {
      URL url = getResource(name);

      if (url == null) {
        LOG.info(name + " not found");
        return null;
      } else {
        LOG.info("found resource " + name + " at " + url);
      }

      return new InputStreamReader(url.openStream());
    } catch (Exception e) {
      return null;
    }
  }

  protected synchronized Properties getProps() {
    if (properties == null) {
      properties = new Properties();
      loadResources(properties, resources, quietmode);
      if (overlay != null) {
        properties.putAll(overlay);
        for (Map.Entry<Object, Object> item: overlay.entrySet()) {
          updatingResource.put((String) item.getKey(), UNKNOWN_RESOURCE);
        }
      }
    }
    return properties;
  }

  /**
   * Return the number of keys in the configuration.
   *
   * @return number of keys in the configuration.
   */
  public int size() {
    return getProps().size();
  }

  /**
   * Clears all keys from the configuration.
   */
  public void clear() {
    getProps().clear();
    getOverlay().clear();
  }

  private void loadResources(Properties properties,
                             ArrayList resources,
                             boolean quiet) {

    for (Object resource : resources) {
      loadResource(properties, resource, quiet);
    }
  }

  private void loadResource(Properties properties, Object name, boolean quiet) {
    try {
      DocumentBuilderFactory docBuilderFactory
        = DocumentBuilderFactory.newInstance();
      //ignore all comments inside the xml file
      docBuilderFactory.setIgnoringComments(true);

      //allow includes in the xml file
      docBuilderFactory.setNamespaceAware(true);
      try {
        docBuilderFactory.setXIncludeAware(true);
      } catch (UnsupportedOperationException e) {
        LOG.error("Failed to set setXIncludeAware(true) for parser "
                    + docBuilderFactory
                    + ":" + e,
                  e);
      }
      DocumentBuilder builder = docBuilderFactory.newDocumentBuilder();
      Document doc = null;
      Element root = null;

      if (name instanceof URL) {                  // an URL resource
        URL url = (URL) name;
        if (url != null) {
          if (!quiet) {
            LOG.info("parsing " + url);
          }
          doc = builder.parse(url.toString());
        }
      } else if (name instanceof String) {        // a CLASSPATH resource
        URL url = getResource((String) name);
        if (url != null) {
          if (!quiet) {
            LOG.info("parsing " + url);
          }
          doc = builder.parse(url.toString());
        }
      } else if (name instanceof InputStream) {
        try {
          doc = builder.parse((InputStream) name);
        } finally {
          ((InputStream) name).close();
        }
      } else if (name instanceof Element) {
        root = (Element) name;
      }

      if (doc == null && root == null) {
        if (quiet) {
          return;
        }
        throw new RuntimeException(name + " not found");
      }

      if (root == null) {
        root = doc.getDocumentElement();
      }
      if (!"configuration".equals(root.getTagName())) {
        LOG.fatal("bad conf file: top-level element not <configuration>");
      }
      NodeList props = root.getChildNodes();
      for (int i = 0; i < props.getLength(); i++) {
        Node propNode = props.item(i);
        if (!(propNode instanceof Element)) {
          continue;
        }
        Element prop = (Element) propNode;
        if ("configuration".equals(prop.getTagName())) {
          loadResource(properties, prop, quiet);
          continue;
        }
        if (!"property".equals(prop.getTagName())) {
          LOG.warn("bad conf file: element not <property>");
        }
        NodeList fields = prop.getChildNodes();
        String attr = null;
        String value = null;
        boolean finalParameter = false;
        for (int j = 0; j < fields.getLength(); j++) {
          Node fieldNode = fields.item(j);
          if (!(fieldNode instanceof Element)) {
            continue;
          }
          Element field = (Element) fieldNode;
          if ("name".equals(field.getTagName()) && field.hasChildNodes()) {
            attr = ((Text) field.getFirstChild()).getData().trim();
          }
          if ("value".equals(field.getTagName()) && field.hasChildNodes()) {
            value = ((Text) field.getFirstChild()).getData();
          }
          if ("final".equals(field.getTagName()) && field.hasChildNodes()) {
            finalParameter = "true".equals(((Text) field.getFirstChild()).getData());
          }
        }

        // Ignore this parameter if it has already been marked as 'final'
        if (attr != null) {
          if (deprecatedKeyMap.containsKey(attr)) {
            DeprecatedKeyInfo keyInfo = deprecatedKeyMap.get(attr);
            keyInfo.accessed = false;
            for (String key:keyInfo.newKeys) {
              // update new keys with deprecated key's value
              loadProperty(properties, name, key, value, finalParameter);
            }
          } else {
            loadProperty(properties, name, attr, value, finalParameter);
          }
        }
      }

    } catch (IOException e) {
      LOG.fatal("error parsing conf file: " + e);
      throw new RuntimeException(e);
    } catch (DOMException e) {
      LOG.fatal("error parsing conf file: " + e);
      throw new RuntimeException(e);
    } catch (SAXException e) {
      LOG.fatal("error parsing conf file: " + e);
      throw new RuntimeException(e);
    } catch (ParserConfigurationException e) {
      LOG.fatal("error parsing conf file: " + e);
      throw new RuntimeException(e);
    }
  }

  private void loadProperty(Properties properties, Object name, String attr,
                            String value, boolean finalParameter) {
    if (value != null) {
      if (!finalParameters.contains(attr)) {
        properties.setProperty(attr, value);
        updatingResource.put(attr, name.toString());
      } else {
        LOG.warn(name + ":an attempt to override final parameter: " + attr + ";  Ignoring.");
      }
    }
    if (finalParameter) {
      finalParameters.add(attr);
    }
  }

  /**
   * Write out the non-default properties in this configuration to the given
   * {@link java.io.OutputStream}.
   *
   * @param out the output stream to write to.
   */
  public void writeXml(OutputStream out) throws IOException {
    writeXml(new OutputStreamWriter(out));
  }

  /**
   * Write out the non-default properties in this configuration to the given
   * {@link java.io.Writer}.
   *
   * @param out the writer to write to.
   */
  public void writeXml(Writer out) throws IOException {
    Document doc = asXmlDocument();

    try {
      DOMSource source = new DOMSource(doc);
      StreamResult result = new StreamResult(out);
      TransformerFactory transFactory = TransformerFactory.newInstance();
      Transformer transformer = transFactory.newTransformer();

      // Important to not hold Configuration log while writing result, since
      // 'out' may be an HDFS stream which needs to lock this configuration
      // from another thread.
      transformer.transform(source, result);
    } catch (TransformerException te) {
      throw new IOException(te);
    }
  }

  /**
   * Return the XML DOM corresponding to this Configuration.
   */
  private synchronized Document asXmlDocument() throws IOException {
    Document doc;
    try {
      doc =
        DocumentBuilderFactory.newInstance().newDocumentBuilder().newDocument();
    } catch (ParserConfigurationException pe) {
      throw new IOException(pe);
    }
    Element conf = doc.createElement("configuration");
    doc.appendChild(conf);
    conf.appendChild(doc.createTextNode("\n"));
    handleDeprecation(); //ensure properties is set and deprecation is handled
    for (Enumeration e = properties.keys(); e.hasMoreElements();) {
      String name = (String) e.nextElement();
      Object object = properties.get(name);
      String value = null;
      if (object instanceof String) {
        value = (String) object;
      } else {
        continue;
      }
      Element propNode = doc.createElement("property");
      conf.appendChild(propNode);

      if (updatingResource != null) {
        Comment commentNode = doc.createComment(
                                                 "Loaded from " + updatingResource.get(name));
        propNode.appendChild(commentNode);
      }
      Element nameNode = doc.createElement("name");
      nameNode.appendChild(doc.createTextNode(name));
      propNode.appendChild(nameNode);

      Element valueNode = doc.createElement("value");
      valueNode.appendChild(doc.createTextNode(value));
      propNode.appendChild(valueNode);

      conf.appendChild(doc.createTextNode("\n"));
    }
    return doc;
  }

  /**
   *  Writes out all the parameters and their properties (final and resource) to
   *  the given {@link Writer}
   *  The format of the output would be
   *  { "properties" : [ {key1,value1,key1.isFinal,key1.resource}, {key2,value2,
   *  key2.isFinal,key2.resource}... ] }
   *  It does not output the parameters of the configuration object which is
   *  loaded from an input stream.
   * @param out the Writer to write to
   * @throws IOException
   */
  public static void dumpConfiguration(Configuration config,
                                       Writer out) throws IOException {
    JsonFactory dumpFactory = new JsonFactory();
    JsonGenerator dumpGenerator = dumpFactory.createJsonGenerator(out);
    dumpGenerator.writeStartObject();
    dumpGenerator.writeFieldName("properties");
    dumpGenerator.writeStartArray();
    dumpGenerator.flush();
    synchronized (config) {
      for (Map.Entry<Object, Object> item: config.getProps().entrySet()) {
        dumpGenerator.writeStartObject();
        dumpGenerator.writeStringField("key", (String) item.getKey());
        dumpGenerator.writeStringField("value",
                                       config.get((String) item.getKey()));
        dumpGenerator.writeBooleanField("isFinal",
                                        config.finalParameters.contains(item.getKey()));
        dumpGenerator.writeStringField("resource",
                                       config.updatingResource.get(item.getKey()));
        dumpGenerator.writeEndObject();
      }
    }
    dumpGenerator.writeEndArray();
    dumpGenerator.writeEndObject();
    dumpGenerator.flush();
  }

  /**
   * Get the {@link ClassLoader} for this job.
   *
   * @return the correct class loader.
   */
  public ClassLoader getClassLoader() {
    return classLoader;
  }

  /**
   * Set the class loader that will be used to load the various objects.
   *
   * @param classLoader the new class loader.
   */
  public void setClassLoader(ClassLoader classLoader) {
    this.classLoader = classLoader;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("Configuration: ");
    toString(resources, sb);
    return sb.toString();
  }

  private <T> void toString(List<T> resources, StringBuilder sb) {
    ListIterator<T> i = resources.listIterator();
    while (i.hasNext()) {
      if (i.nextIndex() != 0) {
        sb.append(", ");
      }
      sb.append(i.next());
    }
  }

  /**
   * Set the quietness-mode.
   *
   * In the quiet-mode, error and informational messages might not be logged.
   *
   * @param quietmode <code>true</code> to set quiet-mode on, <code>false</code>
   *              to turn it off.
   */
  public synchronized void setQuietMode(boolean quietmode) {
    this.quietmode = quietmode;
  }

  synchronized boolean getQuietMode() {
    return this.quietmode;
  }

  /** For debugging.  List non-default properties to the terminal and exit. */
  public static void main(String[] args) throws Exception {
    new Configuration().writeXml(System.out);
  }

  /**
   * get keys matching the the regex.
   * @param regex
   * @return Map<String,String> with matching keys
   */
  public Map<String, String> getValByRegex(String regex) {
    Pattern p = Pattern.compile(regex);

    Map<String, String> result = new HashMap<String, String>();
    Matcher m;

    for (Map.Entry<Object, Object> item: getProps().entrySet()) {
      if (item.getKey() instanceof String &&
            item.getValue() instanceof String) {
        m = p.matcher((String) item.getKey());
        if (m.find()) { // match
          result.put((String) item.getKey(), (String) item.getValue());
        }
      }
    }
    return result;
  }

  @Override
  public Iterator<Map.Entry<String, String>> iterator() {
    return new ConfigurationIterator();
  }

  /**
   * A unique class which is used as a sentinel value in the caching
   * for getClassByName. {@see Configuration#getClassByNameOrNull(String)}
   */
  private abstract static class NegativeCacheSentinel { }

  private class ConfigurationIterator implements Iterator<Map.Entry<String, String>> {
    private String currentName;
    private Iterator<String> nameIter;

    public ConfigurationIterator() {
      nameIter = getProps().stringPropertyNames().iterator();
    }

    @Override
    public boolean hasNext() {
      return nameIter.hasNext();
    }

    @Override
    public Map.Entry<String, String> next() {
      final String name = nameIter.next();
      currentName = name;

      return new Map.Entry<String, String>() {
        @Override
        public String getKey() {
          return name;
        }

        @Override
        public String getValue() {
          return get(name);
        }

        @Override
        public String setValue(String s) {
          String previous = get(s);
          set(name, s);
          return previous;
        }
      };
    }

    @Override
    public void remove() {
      if (currentName == null) {
        throw new IllegalStateException("No current element, next() must be called prior to remove()");
      }
      unset(currentName);
      // prevent duplicate calls
      currentName = null;
    }
  };
}
