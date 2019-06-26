const merge = require('lodash/merge')

var request = require('request'),
  fs = require('fs'),
  log4js = require('log4js');

const applicationsResolver = {
  Query: {
    async applications(parent, args, context, info) {
      const namespace = args.namespace
      const applications = await(new Promise((resolve, reject) => {
        const options = {
          url: `http://127.0.0.1:11015/v3/namespaces/${namespace}/apps`,
          method: 'GET',
          json: true
        };

        request(options, (err, response, body) => {
          if(err) {
            reject(err)
          }
          else {
           resolve(body);
          }
        })
      }));

      context.namespace = namespace

      return applications;
    }
  }
}

const applicationResolver = {
  Query: {
    async application(parent, args, context, info) {
      return await(new Promise((resolve, reject) => {
        const namespace = args.namespace
        const name = args.name

        const options = {
          url: `http://127.0.0.1:11015/v3/namespaces/${namespace}/apps/${name}`,
          method: 'GET',
          json: true
        };

        request(options, (err, response, body) => {
          if(err) {
            reject(err)
          }
          else {
           resolve(body);
          }
        })
      }));
    }
  }
}

const applicationDetailResolver = {
  ApplicationRecord: {
    async applicationDetail(parent, args, context, info) {
      return await(new Promise((resolve, reject) => {
        const namespace = context.namespace
        const name = parent.name

        const options = {
          url: `http://127.0.0.1:11015/v3/namespaces/${namespace}/apps/${name}`,
          method: 'GET',
          json: true
        };

        request(options, (err, response, body) => {
          if(err) {
            reject(err)
          }
          else {
           resolve(body);
          }
        })
      }));
    }
  }
}

const applicationResolvers = merge(applicationsResolver,
                                   applicationResolver,
                                   applicationDetailResolver)

module.exports = {
	applicationResolvers
}