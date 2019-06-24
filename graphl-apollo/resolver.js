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

//    TODO how to get the url and not hardcode it
      request(options, (err, response, body) => {
        if(err) {
          // TODO this is crashing the node server
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
//    TODO how to get the url and not hardcode it
      request(options, (err, response, body) => {
        if(err) {
          // TODO this is crashing the node server
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

//    TODO how to get the url and not hardcode it
      request(options, (err, response, body) => {
        if(err) {
          // TODO this is crashing the node server
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

//    TODO how to get the url and not hardcode it
      request(options, (err, response, body) => {
        if(err) {
          // TODO this is crashing the node server
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

const resolvers = merge(applicationsResolver, namespacesResolver, applicationResolver, applicationDetailResolver)

module.exports = {
	resolvers
}