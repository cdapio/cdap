const merge = require('lodash/merge')

const productMocks = [{ id: 1, name: 'Product A', shortDescription: 'First product.' }, { id: 2, name: 'Product B', shortDescription: 'Second product.' }]
const variantMocks = [{ id: 1, name: 'Variant A', shortDescription: 'First variant.' }, { id: 2, name: 'Variant B', shortDescription: 'Second variant.' }]

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

const resolvers = merge(productResolver, variantResolver)

module.exports = {
	resolvers
}