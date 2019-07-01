var request = require('request'),
  urlHelper = require('../../server/url-helper'),
  cdapConfigurator = require('../../cdap-config.js'),
  resolversCommon = require('./resolvers-common.js');

var cdapConfig;
cdapConfigurator.getCDAPConfig()
  .then(function (value) {
    cdapConfig = value;
  });

const metadataResolver = {
  ApplicationDetail: {
    metadata: async (parent, args, context, info) => {
      return await (new Promise((resolve, reject) => {
        const namespace = context.namespace
        const name = parent.name
        const options = resolversCommon.getGETRequestOptions();
        options['url'] = urlHelper.constructUrl(cdapConfig, `/v3/namespaces/${namespace}/apps/${name}/metadata/tags\?responseFormat=v6`);

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

const metadataResolvers = metadataResolver;

module.exports = {
  metadataResolvers
}
