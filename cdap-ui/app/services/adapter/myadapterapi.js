angular.module(PKG.name + '.services')
  .factory('myAdapterApi', function($resource, myHelpers, myCdapUrl) {
    var url = myCdapUrl.constructUrl,
        templatePath = '/templates',
        adapterPath = '/namespaces/:namespace/adapters/:adapter',
        sourcePath = '/templates/:adapterType/extensions/source',
        sinkPath = '/templates/:adapterType/extensions/sink',
        transformPath = '/templates/:adapterType/extensions/transform',
        basepath = '/namespaces/:namespace/adapters';

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

        list: myHelpers.getConfig('GET', 'REQUEST', basepath, true),
        pollStatus: myHelpers.getConfig('GET', 'POLL', basepath + '/:app/status'),
        delete: myHelpers.getConfig('DELETE', 'REQUEST', basepath + '/:app'),
        runs: myHelpers.getConfig('GET', 'REQUEST', basepath + '/:app/runs', true),
        get: myHelpers.getConfig('GET', 'REQUEST', basepath + '/:app'),
        datasets: myHelpers.getConfig('GET', 'REQUEST', basepath + '/:app/datasets', true),
        streams: myHelpers.getConfig('GET', 'REQUEST', basepath + '/:app/streams', true)
      }
    );
  });
