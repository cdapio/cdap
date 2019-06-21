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
  # ### GET products
  #
  # _Arguments_
  # - **id**: Product's id (optional)
  products(id: Int): [Product]
  # ### GET variants
  #
  # _Arguments_
  # - **id**: Variant's id (optional)
  variants(id: Int): [Variant]
}
`

module.exports = {
	typeDefs
}