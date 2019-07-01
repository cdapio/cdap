var request = require('request'),
  urlHelper = require('../../server/url-helper'),
  cdapConfigurator = require('../../cdap-config.js');

var cdapConfig;
cdapConfigurator.getCDAPConfig()
  .then(function (c) {
    cdapConfig = c;
  });

const namespacesResolver = {
  Query: {
    namespaces: async (parent, args, context, info) => {
      return await (new Promise((resolve, reject) => {
        const options = {
          url: urlHelper.constructUrl(cdapConfig, '/v3/namespaces'),
          method: 'GET',
          json: true
        };

        request(options, (err, response, body) => {
          if (err) {
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
