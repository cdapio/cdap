const typeDefs = `

type Product {

  id: ID!

  name: String!

  shortDescription: String
}

type Variant {

  id: ID!

  name: String!

  shortDescription: String
}

type Namespace {

  name: String

#  description: String

#  generation: String

}

type Query {

  products(id: Int): [Product]

  variants(id: Int): [Variant]

  namespaces: [Namespace]!
}
`

module.exports = {
	typeDefs
}