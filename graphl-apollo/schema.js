const typeDefs = `

type Query {

  namespaces: [Namespace]

  # TODO want to limit the number of apps returned
  applications(namespace: String = "default"): [ApplicationRecord]!

  application(namespace: String = "default", name: String!): ApplicationDetail!
}

type Namespace {

  name: String

  description: String

  generation: String

  config: String

}

type ApplicationRecord {

  type: String!

  name: String!

  version: String!

  description: String!

  artifact: ArtifactSummary!

  ownerPrincipal: String

  # Field added for composition
  applicationDetail: ApplicationDetail!
}

type ArtifactSummary {

  name: String!

  version: String!

  scope: String!
}

type ApplicationDetail {

  name: String!

  appVersion: String!

  description: String!

  configuration: String!

  # List<DatasetDetail> datasets;

  programs(type: String): [ProgramRecord]!

  # List<PluginDetail> plugins;

  artifact: ArtifactSummary!

  ownerPrincipal: String

  # Field added for composition
  metadata: Metadata
}

type Metadata {

  # MetadataEntity metadataEntity;

  # MetadataScope scope;

  # Map<String, String> properties;

  tags: [Tag]!

  # properties: [Properties]
}

type Tag {

  name: String!

  scope: String!
}

interface ProgramRecord {

  type: String!

  app: String!

  name: String!

  description: String!
}

type MapReduce implements ProgramRecord {

  type: String!

  app: String!

  name: String!

  description: String!
}

type Workflow implements ProgramRecord {

  type: String!

  app: String!

  name: String!

  description: String!

  runs: [RunRecord]!

  # Field added for composition
  schedules: [ScheduleDetail]!
}

type RunRecord {

  runid: String!

  starting: String!

  start: String!

  end: String!

  status: String!

  # properties

  # ProgramRunCluster cluster;

  # profileId: String
}

type ScheduleDetail {

  namespace: String!

  application: String!

  applicationVersion: String!

  name: String!

  description: String!

  # ScheduleProgramInfo program;

  # Map<String, String> properties;

  # Trigger trigger;

  # List<? extends Constraint> constraints;

  # Long timeoutMillis;

  status: String!

  # Field added for composition
  # time: [String]!
}
`

module.exports = {
	typeDefs
}