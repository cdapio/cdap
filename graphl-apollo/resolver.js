const merge = require('lodash/merge')

var request = require('request'),
  fs = require('fs'),
  log4js = require('log4js');


const productMocks = [{ id: 1, name: 'Product A', shortDescription: 'First product.' }, { id: 2, name: 'Product B', shortDescription: 'Second product.' }]

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

namespacesResolver = {
  Query: {
    async namespaces(parent, args, context, info) {
    const namespaces = await(new Promise((resolve, reject) => {
      request('http://127.0.0.1:11015/v3/namespaces', (err, response, body) => {
        if(err) {
          reject(err)
        }
        else {
         resolve(JSON.parse(body));
        }
      })
    }));

    return namespaces;
    }
  }
}

const resolvers = merge(productResolver, namespacesResolver)

module.exports = {
	resolvers
}