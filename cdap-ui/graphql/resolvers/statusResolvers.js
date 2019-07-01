var urlHelper = require('../../server/url-helper'),
  cdapConfigurator = require('../../cdap-config.js'),
  resolversCommon = require('./resolvers-common.js');

var cdapConfig;
cdapConfigurator.getCDAPConfig()
  .then(function (value) {
    cdapConfig = value;
  });

const statusResolver = {
  Query: {
    status: async (parent, args, context, info) => {
      const options = resolversCommon.getGETRequestOptions();
      options['url'] = urlHelper.constructUrl(cdapConfig, '/ping');

      const status = await resolversCommon.requestPromiseWrapper(options);

      return status.trim();
    }
  }
}

const statusResolvers = statusResolver;

module.exports = {
  statusResolvers
}
