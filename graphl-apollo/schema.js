const typeDefs = `

type Product {

  id: ID!

  name: String!

  shortDescription: String
}

type Namespace {

  name: String

  description: String

  generation: String

  config: String

}

type Query {

  products(id: Int): [Product]
  
  namespaces: [Namespace]
}
`

module.exports = {
	typeDefs
}