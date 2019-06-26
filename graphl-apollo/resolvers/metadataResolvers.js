var request = require('request'),
  fs = require('fs'),
  log4js = require('log4js');

const metadataResolver = {
  ApplicationDetail: {
    async metadata(parent, args, context, info) {
      return await(new Promise((resolve, reject) => {
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
    }
  }
}

const metadataResolvers = metadataResolver

module.exports = {
	metadataResolvers
}