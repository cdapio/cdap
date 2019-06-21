const { makeExecutableSchema } = require('graphql-tools')
const _ = require('lodash')

const schema = `
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
const productMocks = [{ id: 1, name: 'Product A', shortDescription: 'First product.' }, { id: 2, name: 'Product B', shortDescription: 'Second product.' }]

const productResolver = {
  Query: {
    products(root, { id }, context) {
      const results = id ? productMocks.filter(p => p.id == id) : productMocks
      if (results.length > 0)
        return results
      else
        throw new Error(`Product with id ${id} does not exist.`)
    }
  }
}

const variantMocks = [{ id: 1, name: 'Variant A', shortDescription: 'First variant.' }, { id: 2, name: 'Variant B', shortDescription: 'Second variant.' }]

const variantResolver = {
  Query: {
    variants(root, { id }, context) {
      const results = id ? variantMocks.filter(p => p.id == id) : variantMocks
      if (results.length > 0)
        return results
      else
        throw new Error(`Variant with id ${id} does not exist.`)
    }
  }
}

const executableSchema = {
	typeDefs: schema,
	resolvers: _.merge(productResolver, variantResolver)
}

module.exports = {
	executableSchema
}