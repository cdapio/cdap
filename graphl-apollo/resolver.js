//    TODO how to get the url and not hardcode it
const merge = require('lodash/merge')

var request = require('request'),
  fs = require('fs'),
  log4js = require('log4js');

applicationsResolver = {
  Query: {
    async applications(parent, args, context, info) {
    const applications = await(new Promise((resolve, reject) => {
      namespace = args.namespace

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

namespacesResolver = {
  Query: {
    async namespaces(parent, args, context, info) {
    const options = {
        url: 'http://127.0.0.1:11015/v3/namespaces',
        method: 'GET',
        json: true
    };

    const namespaces = await(new Promise((resolve, reject) => {
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

applicationResolver = {
  Query: {
    async application(parent, args, context, info) {
    const application = await(new Promise((resolve, reject) => {
      namespace = args.namespace
      name = args.name

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

applicationDetailResolver = {
  ApplicationRecord: {
    async applicationDetail(parent, args, context, info) {
    const application = await(new Promise((resolve, reject) => {
      namespace = context.namespace
      name = parent.name

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

metadataResolver = {
  ApplicationDetail: {
    async metadata(parent, args, context, info) {
    const metadata = await(new Promise((resolve, reject) => {
      namespace = context.namespace
      name = parent.name

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

programsTypeResolver = {
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

programsResolver = {
  ApplicationDetail: {
    async programs(parent, args, context, info) {
    const program = await(new Promise((resolve, reject) => {
      programs = parent.programs
      type = args.type

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

runRecordsResolver = {
  Workflow: {
    async runs(parent, args, context, info) {
    const runs = await(new Promise((resolve, reject) => {
      namespace = context.namespace
      name = parent.app
      workflow = parent.name

      const options = {
//        url: `http://127.0.0.1:11015/v3/namespaces/${namespace}/apps/${name}/workflows/${workflow}/runs?%s`,
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

const resolvers = merge(applicationsResolver, namespacesResolver, applicationResolver, applicationDetailResolver, metadataResolver, programsTypeResolver, programsResolver, runRecordsResolver)

module.exports = {
	resolvers
}