package com.continuuity.common.service.distributed;

import com.google.common.base.Function;
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
public class ApplicationMasterSpecification {

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
  private List<TaskSpecification> taskSpecifications = Lists.newArrayList();

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
   * Hook that gets called when the flow is stopping.
   */
  private Function<Void, Void> onShutDownHook;

  /**
   * Hook that gets invoked when the flow is started.
   */
  private Function<Void, Void> onStartupHook;

  /**
   * tracking url set for this application master.
   */
  private String trackingUrl;

  private ApplicationMasterSpecification(Configuration configuration) {
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
  public List<TaskSpecification> getTaskSpecifications() {
    return taskSpecifications;
  }

  private void setTaskSpecifications(List<TaskSpecification> taskSpecifications) {
    this.taskSpecifications = taskSpecifications;
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

  /**
   * Returns the shutdown hook invoked when the flow is being shutdown.
   *
   * @return an instance of shutdown hook if set else null.
   */
  public Function<Void, Void> getOnShutdownHook() {
    return onShutDownHook;
  }

  private void setOnShutDownHook(Function<Void, Void> onShutDownHook) {
    this.onShutDownHook = onShutDownHook;
  }

  /**
   * Returns the start up hook that is invoked after the app master has
   * completely started.
   *
   * @return an instance of startup hook if set else null.
   */
  public Function<Void, Void> getOnStartupHook() {
    return onStartupHook;
  }

  private void setOnStartupHook(Function<Void, Void> onStartupHook) {
    this.onStartupHook = onStartupHook;
  }

  public static class Builder {
    private Configuration configuration;
    private ApplicationAttemptId attemptId;
    private List<TaskSpecification> taskSpecifications = Lists.newArrayList();
    private int allowedFailures = 10;
    private String hostname = "";
    private int clientPort = 0;
    private String trackingUrl = "";
    private Function<Void, Void> onShutdownHook = null;
    private Function<Void, Void> onStartupHook = null;

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

    public Builder addContainerGroupSpecification(TaskSpecification cgp) {
      taskSpecifications.add(cgp);
      return this;
    }

    public Builder setHostname(String hostname) {
      this.hostname = hostname;
      return this;
    }

    public Builder setShutdownHook(Function<Void, Void> onShutdownHook) {
      this.onShutdownHook = onShutdownHook;
      return this;
    }

    public Builder setStartupHook(Function<Void, Void> onStartupHook) {
      this.onStartupHook = onStartupHook;
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

    public ApplicationMasterSpecification create() {
      ApplicationMasterSpecification amp = new ApplicationMasterSpecification(configuration);
      amp.setApplicationAttemptId(attemptId);
      amp.setTaskSpecifications(taskSpecifications);
      amp.setAllowedFailures(allowedFailures);
      amp.setHostname(hostname);
      amp.setClientPort(clientPort);
      amp.setTrackingUrl(trackingUrl);
      amp.setConfiguration(configuration);
      amp.setOnShutDownHook(onShutdownHook);
      amp.setOnStartupHook(onStartupHook);
      return amp;
    }
  }
}
