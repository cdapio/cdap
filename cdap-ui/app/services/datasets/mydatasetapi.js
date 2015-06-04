angular.module(PKG.name + '.services')
  .factory('myDatasetApi', function(myCdapUrl, $resource, myAuth, myHelpers) {

    var url = myCdapUrl.constructUrl,
        listPath = '/namespaces/:namespace/data/datasets',
        basepath = '/namespaces/:namespace/data/datasets/:datasetId';

    return $resource(
      url({ _cdapPath: basepath }),
    {
      namespace: '@namespace',
      datasetId: '@datasetId'
    },
    {
      list: myHelpers.getConfig('GET', 'REQUEST', listPath, true),
      delete: myHelpers.getConfig('DELETE', 'REQUEST', basepath),
      truncate: myHelpers.getConfig('POST', 'REQUEST', basepath + '/admin/truncate'),
      programsList: myHelpers.getConfig('GET', 'REQUEST', basepath + '/programs', true)
    });
  });
