const typeDefs = `

type Query {

  namespaces: [Namespace]

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
  # applicationDetail: ApplicationDetail!
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

  # programs(type: String): [ProgramRecord]!

  # List<PluginDetail> plugins;

  artifact: ArtifactSummary!

  ownerPrincipal: String

  # """
  # Field added for composition
  # """
  # metadata: Metadata
}
`

module.exports = {
	typeDefs
}