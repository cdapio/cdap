var urlHelper = require('../../server/url-helper'),
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
      const options = resolversCommon.getGETRequestOptions();
      options['url'] = urlHelper.constructUrl(cdapConfig, '/v3/namespaces');

      return await resolversCommon.requestPromiseWrapper(options);
    }
  }
}

const namespaceResolvers = namespacesResolver;

module.exports = {
  namespaceResolvers
}
