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
      const namespace = context.namespace
      const name = parent.app
      const workflow = parent.name
      const options = resolversCommon.getGETRequestOptions();
      options['url'] = urlHelper.constructUrl(cdapConfig, `/v3/namespaces/${namespace}/apps/${name}/workflows/${workflow}/runs`);

      return await resolversCommon.requestPromiseWrapper(options);
    }
  }
}

const schedulesResolver = {
  Workflow: {
    schedules: async (parent, args, context, info) => {
      const namespace = context.namespace
      const name = parent.app
      const workflow = parent.name
      const options = resolversCommon.getGETRequestOptions();
      options['url'] = urlHelper.constructUrl(cdapConfig, `/v3/namespaces/${namespace}/apps/${name}/workflows/${workflow}/schedules`);

      context.workflow = workflow

      return await resolversCommon.requestPromiseWrapper(options);
    }
  }
}

const nextRuntimesResolver = {
  ScheduleDetail: {
    nextRuntimes: async (parent, args, context, info) => {
      const namespace = context.namespace
      const name = parent.application
      const workflow = context.workflow
      const options = resolversCommon.getGETRequestOptions();
      options['url'] = urlHelper.constructUrl(cdapConfig, `/v3/namespaces/${namespace}/apps/${name}/workflows/${workflow}/nextruntime`);

      const times = await resolversCommon.getGETRequestOptions(options);

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
