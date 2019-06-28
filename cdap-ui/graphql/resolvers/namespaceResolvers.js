var request = require('request'),
  fs = require('fs'),
  log4js = require('log4js');

const namespacesResolver = {
  Query: {
    async namespaces(parent, args, context, info) {
      return await(new Promise((resolve, reject) => {
        const options = {
          url: 'http://127.0.0.1:11015/v3/namespaces',
          method: 'GET',
          json: true
        };

        request(options, (err, response, body) => {
          if(err) {
            reject(err);
          }
          else {
           resolve(body);
          }
        });
      }));
    }
  }
}

const namespaceResolvers = namespacesResolver;

module.exports = {
	namespaceResolvers
}