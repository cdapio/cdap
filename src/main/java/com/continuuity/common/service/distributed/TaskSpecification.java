
package com.continuuity.common.service.distributed;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * The task specification has two parts to it, namely Resource specification and
 * Execution specification.
 */
public class TaskSpecification {

  private String user;
  private int memory;
  private int priority;
  private int numInstances;
  private Map<String, String> env;
  private List<String> commands;
  private Map<String, LocalResource> namedLocalResources;
  private Map<String, String> meta;
  private String id;


  /**
   * Returns the task id associated
   *
   * @return The ID of the associated task.
   */
  public String getId() {
    return id;
  }

  /**
   * Set the id of the task
   *
   * @param id The id of the task
   */
  private void setId(String id) {
    this.id = id;
  }

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
   * @param clusterMax max resource available in cluster
   * @return Resource to be used by this container.
   */
  public Resource getContainerResource(Resource clusterMin, Resource clusterMax) {
    Resource rsrc = Records.newRecord(Resource.class);
    rsrc.setMemory(this.memory);
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

  /**
   * Resource required by the container locally.
   *
   * @return resources needed by the container.
   */
  public Map<String, LocalResource> getNamedLocalResources() {
    return namedLocalResources;
  }

  private void setNamedLocalResources(Map<String, LocalResource> namedLocalResources ){
    this.namedLocalResources = namedLocalResources;
  }

  /**
   * Returns meta value associated with the key.
   *
   * @param key for which the value is to be retrieved.
   * @return value if present; else null.
   */
  public String getMeta(String key) {
    if(meta.containsKey(key)) {
      return meta.get(key);
    }
    return null;
  }

  private void setMeta(Map<String, String> meta) {
    this.meta = meta;
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
      .add("id", id)
      .add("user", user)
      .add("memory", memory)
      .add("priority", priority)
      .add("numinstances", numInstances)
      .add("env", env)
      .add("commands", commands)
      .add("resource", namedLocalResources)
      .toString();
  }


  public static class Builder {
    private String user = "";
    private int memory = 128;
    private int priority = 0;
    private int numInstances = 1;
    private Map<String, String> env = Maps.newHashMap();
    private List<String> commands = Lists.newArrayList();
    private Map<String, String> namedResources = Maps.newHashMap();
    private final Configuration configuration;
    private final Map<String, String> meta = Maps.newHashMap();
    private String id;

    public Builder(Configuration configuration) {
      this.configuration = configuration;
    }

    public Builder setId(String id) {
      this.id = id;
      return this;
    }

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
      Preconditions.checkNotNull(command);
      commands.add(command);
      return this;
    }

    public Builder addNamedResource(String name, String resource) {
      Preconditions.checkNotNull(name);
      Preconditions.checkNotNull(resource);
      namedResources.put(name, resource);
      return this;
    }

    public Builder addMeta(String key, String value) {
      meta.put(key, value);
      return this;
    }

    public TaskSpecification create() throws IOException {
      TaskSpecification cgp = new TaskSpecification();
      cgp.setUser(user);
      cgp.setMemory(memory);
      cgp.setPriority(priority);
      cgp.setNumInstances(numInstances);
      cgp.setEnvironment(env);
      cgp.setCommands(commands);
      cgp.setId(id);
      cgp.setMeta(meta);

      Map<String, LocalResource> localResourceMap = Maps.newHashMap();
      FileSystem fs = FileSystem.get(configuration);
      for(Map.Entry<String, String> entry : namedResources.entrySet()) {
        LocalResource localResource = Records.newRecord(LocalResource.class);
        Path path = new Path(entry.getValue());
        FileStatus stat = fs.getFileStatus(path);
        localResource.setResource(ConverterUtils.getYarnUrlFromPath(fs.makeQualified(path)));
        localResource.setType(LocalResourceType.FILE);
        localResource.setSize(stat.getLen());
        localResource.setTimestamp(stat.getModificationTime());
        localResource.setVisibility(LocalResourceVisibility.APPLICATION);
        localResourceMap.put(entry.getKey(), localResource);
      }
      cgp.setNamedLocalResources(localResourceMap);
      return cgp;
    }
  }
}
