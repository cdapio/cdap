angular.module(PKG.name + '.services')
  .factory('myAdapterApi', function($resource, myHelpers) {
    var templatePath = '/templates',
        adapterPath = '/namespaces/:namespace/adapters/:adapter',
        sourcePath = '/templates/:adapterType/extensions/source',
        sinkPath = '/templates/:adapterType/extensions/sink',
        transformPath = '/templates/:adapterType/extensions/transform';

    return $resource(
      '',
      {

      },
      {
        save: myHelpers.getConfig('PUT', 'REQUEST', adapterPath),
        fetchTemplates: myHelpers.getConfig('GET', 'REQUEST', templatePath, true),
        fetchSources: myHelpers.getConfig('GET', 'REQUEST', sourcePath, true),
        fetchSinks: myHelpers.getConfig('GET', 'REQUEST', sinkPath, true),
        fetchTransforms: myHelpers.getConfig('GET', 'REQUEST', transformPath, true),
        fetchSourceProperties: myHelpers.getConfig('GET', 'REQUEST', sourcePath + '/plugins/:source', true),
        fetchSinkProperties: myHelpers.getConfig('GET', 'REQUEST', sinkPath + '/plugins/:sink', true),
        fetchTransformProperties: myHelpers.getConfig('GET', 'REQUEST', transformPath + '/plugins/:transform', true),

        list: myHelpers.getConfig('GET', 'REQUEST', '/namespaces/:namespace/adapters', true),
        pollStatus: myHelpers.getConfig('GET', 'POLL', adapterPath + '/status'),
        stopPollStatus: myHelpers.getConfig('GET', 'POLL-STOP', adapterPath + '/status'),
        delete: myHelpers.getConfig('DELETE', 'REQUEST', adapterPath),
        runs: myHelpers.getConfig('GET', 'REQUEST', adapterPath + '/runs', true),
        get: myHelpers.getConfig('GET', 'REQUEST', adapterPath),
        datasets: myHelpers.getConfig('GET', 'REQUEST', adapterPath + '/datasets', true),
        streams: myHelpers.getConfig('GET', 'REQUEST', adapterPath + '/streams', true),
        action: myHelpers.getConfig('POST', 'REQUEST', adapterPath + '/:action')
      }
    );
  });
