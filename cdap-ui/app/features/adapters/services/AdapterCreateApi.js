angular.module(PKG.name + '.feature.adapters')
  .factory('AdapterApiFactory', function($resource, myHelpers, myCdapUrl) {
    var url = myCdapUrl.constructUrl,
        templatePath = '/templates',
        sourcePath = '/templates/:adapterType/extensions/source',
        sinkPath = '/templates/:adapterType/extensions/sink',
        transformPath = '/templates/:adapterType/extensions/transform';

    return $resource(
      '',
      {

      },
      {
        fetchTemplates: myHelpers.getConfig('GET', 'REQUEST', templatePath, true),
        fetchSources: myHelpers.getConfig('GET', 'REQUEST', sourcePath, true),
        fetchSinks: myHelpers.getConfig('GET', 'REQUEST', sinkPath, true),
        fetchTransforms: myHelpers.getConfig('GET', 'REQUEST', transformPath, true)
      }
    );
  });
