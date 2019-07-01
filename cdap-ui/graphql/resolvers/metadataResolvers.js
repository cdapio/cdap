var request = require('request'),
  urlHelper = require('../../server/url-helper'),
  cdapConfigurator = require('../../cdap-config.js');

var cdapConfig;
cdapConfigurator.getCDAPConfig()
  .then(function (c) {
    cdapConfig = c;
  });

const metadataResolver = {
  ApplicationDetail: {
    metadata: async (parent, args, context, info) => {
      return await (new Promise((resolve, reject) => {
        const namespace = context.namespace
        const name = parent.name

        const options = {
          url: urlHelper.constructUrl(cdapConfig, `/v3/namespaces/${namespace}/apps/${name}/metadata/tags\?responseFormat=v6`),
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

const metadataResolvers = metadataResolver;

module.exports = {
  metadataResolvers
}
