const merge = require('lodash/merge')

var request = require('request'),
  fs = require('fs'),
  log4js = require('log4js');

const runsResolver = {
  Workflow: {
    async runs(parent, args, context, info) {
      return await(new Promise((resolve, reject) => {
        const namespace = context.namespace
        const name = parent.app
        const workflow = parent.name

        const options = {
          url: `http://127.0.0.1:11015/v3/namespaces/${namespace}/apps/${name}/workflows/${workflow}/runs`,
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

const schedulesResolver = {
  Workflow: {
    async schedules(parent, args, context, info) {
      const schedules = await(new Promise((resolve, reject) => {
        const namespace = context.namespace
        const name = parent.app
        workflow = parent.name

        const options = {
          url: `http://127.0.0.1:11015/v3/namespaces/${namespace}/apps/${name}/workflows/${workflow}/schedules`,
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

      context.workflow = workflow

      return schedules;
    }
  }
}

const nextRuntimesResolver = {
  ScheduleDetail: {
    async nextRuntimes(parent, args, context, info) {
      const times = await(new Promise((resolve, reject) => {
        const namespace = context.namespace
        const name = parent.application
        const workflow = context.workflow

        const options = {
          url: `http://127.0.0.1:11015/v3/namespaces/${namespace}/apps/${name}/workflows/${workflow}/nextruntime`,
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

      const nextRuntimes = []

      for(let i = 0; i < times.length; i++) {
        nextRuntimes.push(times[i].time)
      }

      return nextRuntimes
    }
  }
}

const scheduleResolvers = merge(runsResolver,
                               schedulesResolver,
                               nextRuntimesResolver)

module.exports = {
	scheduleResolvers
}