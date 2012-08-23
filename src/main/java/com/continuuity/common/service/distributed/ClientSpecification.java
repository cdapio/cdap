package com.continuuity.common.service.distributed;

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;
import org.mortbay.log.Log;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 *
 */
public class ClientSpecification {

  /**
   * Name of the application.
   */
  private String applicationName;

  /**
   * Name of the queue the application would run on.
   */
  private String queue;

  /**
   * Instance of configuration object.
   */
  private Configuration configuration;

  /**
   * List of commands to start application master.
   */
  private List<String> commands;

  /**
   * Maps of variables to values.
   */
  private Map<String, String> environment;

  /**
   * Memory used by AM.
   */
  private int memory;

  /**
   * Named local resources.
   */
  private Map<String, String> namedLocalResources;

  /**
   * Preventing from ClientSpecification to be constructed directly.
   */
  private ClientSpecification(){}

  /**
   * Name of the application.
   *
   * @return name of the application.
   */
  public String getApplicationName() {
    return applicationName;
  }

  private void setApplicationName(String applicationName) {
    this.applicationName = applicationName;
  }

  /**
   * Returns name of the queue.
   *
   * @return name of the queue.
   */
  public String getQueue() {
    return queue;
  }

  private void setQueue(String queue) {
    this.queue = queue;
  }

  /**
   * Returns the instance of configuration object.
   *
   * @return instance of configuration object.
   */
  public Configuration getConfiguration() {
    return configuration;
  }

  private void setConfiguration(Configuration configuration) {
    this.configuration = configuration;
  }

  /**
   * Returns the commands used to start application master.
   *
   * @return commands to start application master.
   */
  public List<String> getCommands() {
    return commands;
  }

  private void setCommands(List<String> commands) {
    this.commands = commands;
  }

  /**
   * Returns the environment variables and their values.
   *
   * @return map of variable names and values.
   */
  public Map<String, String> getEnvironment() {
    return environment;
  }

  private void setEnvironment(Map<String, String> environment) {
    this.environment = environment;
  }

  /**
   * Returns the amount of memory used by AM.
   *
   * @return memory used by AM.
   */
  public int getMemory() {
    return memory;
  }

  private void setMemory(int memory) {
    this.memory = memory;
  }

  /**
   * Resource required by the container locally.
   *
   * @return resources needed by the container.
   */
  public Map<String, String> getNamedLocalResources() {
    return namedLocalResources;
  }

  private void setNamedLocalResources(Map<String, String> namedLocalResources ){
    this.namedLocalResources = namedLocalResources;
  }

  public static class Builder {
    private String applicationName = "no-app-name";
    private String queue = "default";
    private Configuration configuration = new Configuration();
    private List<String> commands = Lists.newArrayList();
    private Map<String, String> environment = Maps.newHashMap();
    private int memory = 512;
    private Map<String, String> namedResources = Maps.newHashMap();

    public void setApplicationName(String applicationName) {
      this.applicationName = applicationName;
    }

    public void setQueue(String queue) {
      this.queue = queue;
    }

    public void setConfiguration(Configuration configuration) {
      this.configuration = configuration;
    }

    public void addCommand(String command) {
      commands.add(command);
    }

    public void addEnvironment(String name, String value) {
      environment.put(name, value);
    }

    public void setMemory(int memory) {
      this.memory = memory;
    }

    public Builder addNamedResource(String name, String resource) {
      namedResources.put(name, resource);
      return this;
    }

    public ClientSpecification create() throws IOException {
      ClientSpecification specification = new ClientSpecification();
      specification.setApplicationName(applicationName);
      specification.setQueue(queue);
      specification.setConfiguration(configuration);
      specification.setCommands(commands);
      specification.setEnvironment(environment);
      specification.setMemory(memory);
      specification.setNamedLocalResources(namedResources);
      return specification;
    }
  }
}
