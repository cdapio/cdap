const merge = require('lodash/merge')

var request = require('request'),
  fs = require('fs'),
  log4js = require('log4js');


const productMocks = [{ id: 1, name: 'Product A', shortDescription: 'First product.' }, { id: 2, name: 'Product B', shortDescription: 'Second product.' }]
const variantMocks = [{ id: 1, name: 'Variant A', shortDescription: 'First variant.' }, { id: 2, name: 'Variant B', shortDescription: 'Second variant.' }]

const productResolver = {
  Query: {
    products(root, args, context, info) {
      const id = args.id
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
    variants(root, args, context, info) {
      const id = args.id
      const results = id ? variantMocks.filter(p => p.id == id) : variantMocks



//      return new Promise((resolve, reject) => {
//      request(url, options, (response) => {
//        resolve(response);
//
//        reject(response);
//      })
//      });




      if (results.length > 0)
        return results
      else
        throw new Error(`Variant with id ${id} does not exist.`)
    }
  }
}

const namespacesResolver = {
  Query: {
    namespaces(parent, args, context, info) {
    return new Promise((resolve, reject) => {
      request('http://127.0.0.1:11015/v3/namespaces', (response, body) => {
         resolve(body);
      })
    });
    }
  }
}

const resolvers = merge(productResolver, variantResolver, namespacesResolver)

module.exports = {
	resolvers
}