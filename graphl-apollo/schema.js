const typeDefs = `

type Query {

  namespaces: [Namespace]

  applications(namespace: String = "default"): [ApplicationRecord]!
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

  # artifact: ArtifactSummary!

  ownerPrincipal: String

  # """
  # Field added for composition
  # """
  # applicationDetail: ApplicationDetail!
}
`

module.exports = {
	typeDefs
}