const merge = require('lodash/merge');

var request = require('request'),
  urlHelper = require('../../server/url-helper'),
  cdapConfigurator = require('../../cdap-config.js');

var cdapConfig;
cdapConfigurator.getCDAPConfig()
  .then(function (c) {
    cdapConfig = c;
  });

const applicationsResolver = {
  Query: {
    applications: async (parent, args, context, info) => {
      const namespace = args.namespace
      const applications = await (new Promise((resolve, reject) => {
        const options = {
          url: urlHelper.constructUrl(cdapConfig, `/v3/namespaces/${namespace}/apps`),
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

      context.namespace = namespace

      return applications;
    }
  }
}

const applicationResolver = {
  Query: {
    application: async (parent, args, context, info) => {
      return await (new Promise((resolve, reject) => {
        const namespace = args.namespace
        const name = args.name

        const options = {
          url: urlHelper.constructUrl(cdapConfig, `/v3/namespaces/${namespace}/apps/${name}`),
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

const applicationDetailResolver = {
  ApplicationRecord: {
    async applicationDetail(parent, args, context, info) {
      return await (new Promise((resolve, reject) => {
        const namespace = context.namespace
        const name = parent.name

        const options = {
          url: urlHelper.constructUrl(cdapConfig, `/v3/namespaces/${namespace}/apps/${name}`),
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

const applicationResolvers = merge(applicationsResolver,
  applicationResolver,
  applicationDetailResolver);

module.exports = {
  applicationResolvers
}
