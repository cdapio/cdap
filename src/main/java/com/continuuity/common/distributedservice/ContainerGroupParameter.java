package com.continuuity.common.distributedservice;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.util.Records;

import java.util.List;
import java.util.Map;

/**
 * The parameters that are common to launching both application masters and node tasks via
 * a {@code ContainerLaunchContext} instance.
 */
public class ContainerGroupParameter {

  private String user;
  private int memory;
  private int priority;
  private int numInstances;
  private Map<String, String> env;
  private List<String> commands;

  /**
   * Returns the user the container would run as
   *
   * @return user the container would run as.
   */
  public String getUser() {
    return user;
  }

  private void setUser(String user) {
    this.user = user;
  }

  /**
   * Returns the amount of memory requested by this container in MB.
   *
   * @return amount of memory to be used by this container in MB.
   */
  public int getMemory() {
    return memory;
  }

  private void setMemory(int memory) {
    this.memory = memory;
  }

  /**
   * Returns the resources needed for this job, using the cluster min and max as bounds.
   */
  /**
   * Resource need for this container, it using the min and max bounds of the cluster to determine that.
   *
   * @param clusterMin min resource available in cluster
   * @param clusterMax max resource availabel in cluster
   * @return Resource to be used by this container.
   */
  public Resource getContainerResource(Resource clusterMin, Resource clusterMax) {
    Resource rsrc = Records.newRecord(Resource.class);
    rsrc.setMemory(Math.min(clusterMax.getMemory(),
      Math.max(clusterMin.getMemory(), getMemory())));
    return rsrc;
  }

  /**
   * Returns the priorty of this container.
   *
   * @return priority of container.
   */
  public int getPriority() {
    return priority;
  }

  private void setPriority(int priority) {
    this.priority = priority;
  }

  /**
   * Number of instance of this container to be launched.
   *
   * @return number of container instances.
   */
  public int getNumInstances() {
    return numInstances;
  }

  private void setNumInstances(int numInstances) {
    this.numInstances = numInstances;
  }

  /**
   * Environment variables that will used by the container.
   *
   * @return environment variables for the container.
   */
  public Map<String, String> getEnvironment() {
    return env;
  }

  private void setEnvironment(Map<String, String> env) {
    this.env = env;
  }

  /**
   * Commands to be executed that starts the application within the container.
   *
   * @return command to execute to start an application within the container.
   */
  public List<String> getCommands() {
    return commands;
  }

  private void setCommands(List<String> commands) {
    this.commands = commands;
  }

  public static class Builder {
    private String user;
    private int memory;
    private int priority;
    private int numInstances;
    private Map<String, String> env = Maps.newHashMap();
    private List<String> commands = Lists.newArrayList();

    public Builder setUser(String user) {
      this.user = user;
      return this;
    }

    public Builder setMemory(int memory) {
      this.memory = memory;
      return this;
    }

    public Builder setPriority(int priority) {
      this.priority = priority;
      return this;
    }

    public Builder setNumInstances(int numInstances) {
      this.numInstances = numInstances;
      return this;
    }

    public Builder addEnv(String key, String value) {
      env.put(key, value);
      return this;
    }

    public Builder addCommand(String command) {
      commands.add(command);
      return this;
    }

    public ContainerGroupParameter create() {
      ContainerGroupParameter cgp = new ContainerGroupParameter();
      cgp.setUser(user);
      cgp.setMemory(memory);
      cgp.setPriority(priority);
      cgp.setNumInstances(numInstances);
      cgp.setEnvironment(env);
      cgp.setCommands(commands);
      return cgp;
    }
  }
}
