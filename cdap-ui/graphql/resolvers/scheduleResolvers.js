const merge = require('lodash/merge')

var request = require('request'),
  urlHelper = require('../../server/url-helper'),
  cdapConfigurator = require('../../cdap-config.js'),
  resolversCommon = require('./resolvers-common.js');

var cdapConfig;
cdapConfigurator.getCDAPConfig()
  .then(function (value) {
    cdapConfig = value;
  });

const runsResolver = {
  Workflow: {
    runs: async (parent, args, context, info) => {
      return await (new Promise((resolve, reject) => {
        const namespace = context.namespace
        const name = parent.app
        const workflow = parent.name
        const options = resolversCommon.getGETRequestOptions();
        options['url'] = urlHelper.constructUrl(cdapConfig, `/v3/namespaces/${namespace}/apps/${name}/workflows/${workflow}/runs`);

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

const schedulesResolver = {
  Workflow: {
    schedules: async (parent, args, context, info) => {
      const schedules = await (new Promise((resolve, reject) => {
        const namespace = context.namespace
        const name = parent.app
        const workflow = parent.name
        const options = resolversCommon.getGETRequestOptions();
        options['url'] = urlHelper.constructUrl(cdapConfig, `/v3/namespaces/${namespace}/apps/${name}/workflows/${workflow}/schedules`);

        request(options, (err, response, body) => {
          if (err) {
            reject(err);
          }
          else {
            resolve(body);
          }
        });
      }));

      context.workflow = workflow

      return schedules;
    }
  }
}

const nextRuntimesResolver = {
  ScheduleDetail: {
    nextRuntimes: async (parent, args, context, info) => {
      const times = await (new Promise((resolve, reject) => {
        const namespace = context.namespace
        const name = parent.application
        const workflow = context.workflow
        const options = resolversCommon.getGETRequestOptions();
        options['url'] = urlHelper.constructUrl(cdapConfig, `/v3/namespaces/${namespace}/apps/${name}/workflows/${workflow}/nextruntime`);

        request(options, (err, response, body) => {
          if (err) {
            reject(err);
          }
          else {
            resolve(body);
          }
        });
      }));

      const nextRuntimes = []

      for (let i = 0; i < times.length; i++) {
        nextRuntimes.push(times[i].time)
      }

      return nextRuntimes
    }
  }
}

const scheduleResolvers = merge(runsResolver,
  schedulesResolver,
  nextRuntimesResolver);

module.exports = {
  scheduleResolvers
}
