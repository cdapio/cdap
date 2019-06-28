var request = require('request'),
  fs = require('fs'),
  log4js = require('log4js'),
  urlHelper = require('../../server/url-helper');
cdapConfigurator = require('../../cdapConfigurator.js');

var log = log4js.getLogger('namespaceResolver');

var cdapConfig;
cdapConfigurator.cdapConfig
  .then(function (c) {
    cdapConfig = c;
  });

const namespacesResolver = {
  Query: {
    async namespaces(parent, args, context, info) {
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
