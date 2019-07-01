const merge = require('lodash/merge');

var request = require('request'),
  urlHelper = require('../../server/url-helper'),
  cdapConfigurator = require('../../cdap-config.js'),
  resolversCommon = require('./resolvers-common.js');

var cdapConfig;
cdapConfigurator.getCDAPConfig()
  .then(function (value) {
    cdapConfig = value;
  });

const applicationsResolver = {
  Query: {
    applications: async (parent, args, context, info) => {
      const namespace = args.namespace
      const options = resolversCommon.getGETRequestOptions();
      options['url'] = urlHelper.constructUrl(cdapConfig, `/v3/namespaces/${namespace}/apps`);
      context.namespace = namespace

      return await resolversCommon.requestPromiseWrapper(options);
    }
  }
}

const applicationResolver = {
  Query: {
    application: async (parent, args, context, info) => {
      const namespace = args.namespace
      const name = args.name
      const options = resolversCommon.getGETRequestOptions();
      options['url'] = urlHelper.constructUrl(cdapConfig, `/v3/namespaces/${namespace}/apps/${name}`);

      return await resolversCommon.requestPromiseWrapper(options);
    }
  }
}

const applicationDetailResolver = {
  ApplicationRecord: {
    async applicationDetail(parent, args, context, info) {
      const namespace = context.namespace
      const name = parent.name
      const options = resolversCommon.getGETRequestOptions();
      options['url'] = urlHelper.constructUrl(cdapConfig, `/v3/namespaces/${namespace}/apps/${name}`);

      return await resolversCommon.requestPromiseWrapper(options);
    }
  }
}

const applicationResolvers = merge(applicationsResolver,
  applicationResolver,
  applicationDetailResolver);

module.exports = {
  applicationResolvers
}
