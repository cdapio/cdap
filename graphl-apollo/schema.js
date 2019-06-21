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

type Query {

  products(id: Int): [Product]

  variants(id: Int): [Variant]
}
`

module.exports = {
	typeDefs
}