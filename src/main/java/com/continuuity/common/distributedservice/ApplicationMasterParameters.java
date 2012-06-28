package com.continuuity.common.distributedservice;

import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.util.ConverterUtils;

import java.util.List;
import java.util.Map;

/**
 * The information that the {@code ApplicationMasterService} needs to know in order to
 * setup and manage a YARN application.
 */
public class ApplicationMasterParameters {

  /**
   * Instance of configuration object.
   */
  private Configuration configuration;

  /**
   * Application attempt id.
   */
  private ApplicationAttemptId attemptId;

  /**
   * Collection of container group parameters.
   */
  private List<ContainerGroupParameter> containerGroupParameters = Lists.newArrayList();

  /**
   * Number of failures of containers allowed across all the groups. -1 is unlimited.
   */
  private int allowedFailures = -1;

  /**
   * Hostname the master is requested to be run on. * specifies any host.
   */
  private String hostname;

  /**
   * Client port set for application master.
   */
  private int clientPort;

  /**
   * tracking url set for this application master.
   */
  private String trackingUrl;

  private ApplicationMasterParameters(Configuration configuration) {
    this.configuration = configuration;
  }

  /**
   * Returns the {@code Configuration} instance that should be used for this run.
   */
  public Configuration getConfiguration() {
    return configuration;
  }

  private void setConfiguration(Configuration configuration) {
    this.configuration = configuration;
  }

  /**
   * Returns the attempt ID for this application.
   */
  public ApplicationAttemptId getApplicationAttemptId() {
    return attemptId;
  }

  private void setApplicationAttemptId(ApplicationAttemptId attemptId) {
    this.attemptId = attemptId;
  }

  /**
   * Returns the parameters that will be used to launch the child containers for
   * this application.
   */
  public List<ContainerGroupParameter> getAllContainerGroups() {
    return containerGroupParameters;
  }

  private void setAllContainerGroups(List<ContainerGroupParameter> containerGroupParameters) {
    this.containerGroupParameters = containerGroupParameters;
  }

  /**
   * Returns the number of containers that are allowed to fail before this
   * application shuts itself down automatically.
   */
  public int getAllowedFailures() {
    return allowedFailures;
  }

  private void setAllowedFailures(int allowedFailures) {
    this.allowedFailures = allowedFailures;
  }

  /**
   * Returns the hostname that was set for this application master.
   */
  public String getHostname() {
    return hostname;
  }

  private void setHostname(String hostname) {
    this.hostname = hostname;
  }


  /**
   * Returns the client port that was set for this application master.
   */
  public int getClientPort() {
    return clientPort;
  }

  private void setClientPort(int port) {
    clientPort = port;
  }

  /**
   * Returns the tracking URL that was set for this application master.
   */
  public String getTrackingUrl() {
    return trackingUrl;
  }

  private void setTrackingUrl(String trackingUrl) {
    this.trackingUrl = trackingUrl;
  }

  public class Builder {
    private Configuration configuration;
    private ApplicationAttemptId attemptId;
    private List<ContainerGroupParameter> containerGroupParameters = Lists.newArrayList();
    private int allowedFailures = -1;
    private String hostname = "*";
    private int clientPort = -1;
    private String trackingUrl = null;

    public Builder() {
      Map<String, String> env = System.getenv();
      if (env.containsKey(ApplicationConstants.AM_CONTAINER_ID_ENV)) {
        ContainerId containerId = ConverterUtils.toContainerId(env.get(ApplicationConstants.AM_CONTAINER_ID_ENV));
        this.attemptId = containerId.getApplicationAttemptId();
      } else {
        this.attemptId = null;
      }
    }

    public Builder addConfiguration(Configuration configuration) {
      this.configuration = configuration;
      return this;
    }

    public Builder addContainerGroupParameter(ContainerGroupParameter cgp) {
      containerGroupParameters.add(cgp);
      return this;
    }

    public Builder setHostname(String hostname) {
      this.hostname = hostname;
      return this;
    }

    public Builder setClientPort(int clientPort) {
      this.clientPort = clientPort;
      return this;
    }

    public Builder setTrackingUrl(String trackingUrl) {
      this.trackingUrl = trackingUrl;
      return this;
    }

    public ApplicationMasterParameters create() {
      ApplicationMasterParameters amp = new ApplicationMasterParameters(configuration);
      amp.setApplicationAttemptId(attemptId);
      amp.setAllContainerGroups(containerGroupParameters);
      amp.setAllowedFailures(allowedFailures);
      amp.setClientPort(clientPort);
      amp.setTrackingUrl(trackingUrl);
      return amp;
    }
  }
}
