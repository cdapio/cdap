package com.continuuity.common.zookeeper;

import com.google.common.collect.ImmutableList;
import com.google.common.io.Files;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig;
import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Properties;

@SuppressWarnings("UnusedDeclaration")
class QuorumConfigBuilder {
  private final ImmutableList<InstanceSpecification> instanceSpecs;
  private final boolean fromRandom;

  public QuorumConfigBuilder(Collection<InstanceSpecification> specs) {
    this(specs.toArray(new InstanceSpecification[specs.size()]));
  }

  public QuorumConfigBuilder(InstanceSpecification... specs) {
    fromRandom = (specs == null) || (specs.length == 0);
    instanceSpecs = fromRandom ? ImmutableList.of(InstanceSpecification.newInstanceSpec()) : ImmutableList.copyOf(specs);
  }

  public boolean isFromRandom() {
    return fromRandom;
  }

  public QuorumPeerConfig buildConfig() throws IOException, QuorumPeerConfig.ConfigException {
    return buildConfig(0);
  }

  public InstanceSpecification     getInstanceSpec(int index) {
    return instanceSpecs.get(index);
  }

  public List<InstanceSpecification> getInstanceSpecs() {
    return instanceSpecs;
  }

  public int  size() {
    return instanceSpecs.size();
  }

  public QuorumPeerConfig buildConfig(int instanceIndex) throws IOException, QuorumPeerConfig.ConfigException {
    boolean       isCluster = (instanceSpecs.size() > 1);
    InstanceSpecification  spec = instanceSpecs.get(instanceIndex);

    if ( isCluster ) {
      Files.write(Integer.toString(spec.getServerId()).getBytes(), new File(spec.getDataDirectory(), "myid"));
    }

    Properties properties = new Properties();
    properties.setProperty("initLimit", "10");
    properties.setProperty("syncLimit", "5");
    properties.setProperty("dataDir", spec.getDataDirectory().getCanonicalPath());
    properties.setProperty("clientPort", Integer.toString(spec.getPort()));
    if ( isCluster ) {
      for ( InstanceSpecification thisSpec : instanceSpecs ) {
        properties.setProperty("server." + thisSpec.getServerId(), String.format("localhost:%d:%d", thisSpec.getQuorumPort(), thisSpec.getElectionPort()));
      }
    }

    QuorumPeerConfig config = new QuorumPeerConfig();
    config.parseProperties(properties);
    return config;
  }
}
