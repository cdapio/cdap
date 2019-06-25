//    TODO how to get the url and not hardcode it
const merge = require('lodash/merge')

var request = require('request'),
  fs = require('fs'),
  log4js = require('log4js');

const applicationsResolver = {
  Query: {
    async applications(parent, args, context, info) {
    const applications = await(new Promise((resolve, reject) => {
      const namespace = args.namespace

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

const namespacesResolver = {
  Query: {
    async namespaces(parent, args, context, info) {
    const namespaces = await(new Promise((resolve, reject) => {
      const options = {
        url: 'http://127.0.0.1:11015/v3/namespaces',
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

    return namespaces;
    }
  }
}

const applicationResolver = {
  Query: {
    async application(parent, args, context, info) {
    const application = await(new Promise((resolve, reject) => {
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

    return application;
    }
  }
}

const applicationDetailResolver = {
  ApplicationRecord: {
    async applicationDetail(parent, args, context, info) {
    const application = await(new Promise((resolve, reject) => {
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

    return application;
    }
  }
}

const metadataResolver = {
  ApplicationDetail: {
    async metadata(parent, args, context, info) {
    const metadata = await(new Promise((resolve, reject) => {
      const namespace = context.namespace
      const name = parent.name

      const options = {
        url: `http://127.0.0.1:11015/v3/namespaces/${namespace}/apps/${name}/metadata/tags\?responseFormat=v6`,
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

    return metadata;
    }
  }
}

const programsTypeResolver = {
  ProgramRecord: {
    async __resolveType(parent, args, context, info) {
    const programs = await(new Promise((resolve, reject) => {
      switch(parent.type) {
        case 'Mapreduce': resolve('MapReduce')
        case 'Workflow': resolve('Workflow')
        default: resolve(null)
      }
    }));

    return programs;
    }
  }
}

const programsResolver = {
  ApplicationDetail: {
    async programs(parent, args, context, info) {
    const program = await(new Promise((resolve, reject) => {
      const programs = parent.programs
      const type = args.type

      if(type == null) {
        resolve(programs)
      }
      else {
        typePrograms = programs.filter(
          function(program) {
            return program.type == type
          }
      )

      resolve(typePrograms)
      }
    }));

    return program;
    }
  }
}

const runsResolver = {
  Workflow: {
    async runs(parent, args, context, info) {
    const runs = await(new Promise((resolve, reject) => {
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

    return runs;
    }
  }
}

const schedulesResolver = {
  Workflow: {
    async schedules(parent, args, context, info) {
    const schedules = await(new Promise((resolve, reject) => {
      const namespace = context.namespace
      const name = parent.app
      const workflow = parent.name

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

const resolvers = merge(applicationsResolver, namespacesResolver, applicationResolver, applicationDetailResolver, metadataResolver, programsTypeResolver, programsResolver, runsResolver, schedulesResolver, nextRuntimesResolver)

module.exports = {
	resolvers
}