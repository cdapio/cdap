var request = require('request'),
  urlHelper = require('../../server/url-helper'),
  cdapConfigurator = require('../../cdap-config.js'),
  resolversCommon = require('./resolvers-common.js');

var cdapConfig;
cdapConfigurator.getCDAPConfig()
  .then(function (value) {
    cdapConfig = value;
  });

const namespacesResolver = {
  Query: {
    namespaces: async (parent, args, context, info) => {
      return await (new Promise((resolve, reject) => {
        const options = resolversCommon.getGETRequestOptions();
        options['url'] = urlHelper.constructUrl(cdapConfig, '/v3/namespaces');

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
