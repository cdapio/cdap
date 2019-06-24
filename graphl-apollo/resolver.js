const merge = require('lodash/merge')

var request = require('request'),
  fs = require('fs'),
  log4js = require('log4js');

ApplicationRecordsResolver = {
  Query: {
    async applications(parent, args, context, info) {
    const applications = await(new Promise((resolve, reject) => {
      namespace = args.namespace
//    TODO how to get the url and not hardcode it
      request(`http://127.0.0.1:11015/v3/namespaces/${namespace}/apps`, (err, response, body) => {
        if(err) {
          // TODO this is crashing the node server
          reject(err)
        }
        else {
         resolve(JSON.parse(body));
        }
      })
    }));

    return applications;
    }
  }
}


namespacesResolver = {
  Query: {
    async namespaces(parent, args, context, info) {
    const namespaces = await(new Promise((resolve, reject) => {
//    TODO how to get the url and not hardcode it
      request('http://127.0.0.1:11015/v3/namespaces', (err, response, body) => {
        if(err) {
          // TODO this is crashing the node server
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

const resolvers = merge(namespacesResolver, ApplicationRecordsResolver)

module.exports = {
	resolvers
}