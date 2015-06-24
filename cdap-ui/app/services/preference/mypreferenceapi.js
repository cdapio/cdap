angular.module(PKG.name + '.services')
  .factory('myPreferenceApi', function(myCdapUrl, $resource, myAuth, myHelpers) {

    var url = myCdapUrl.constructUrl,
        basepath = '/namespaces/:namespace';

    return $resource(
      url({ _cdapPath: basepath }),
    {
      namespace: '@namespace',
      appId: '@appId',
      programType: '@programType',
      programId: '@programId',
    },
    {
      getSystemPreference: myHelpers.getConfig('GET', 'REQUEST', '/preferences'),
      setSystemPreference: myHelpers.getConfig('PUT', 'REQUEST', '/preferences'),
      getNamespacePreference: myHelpers.getConfig('GET', 'REQUEST', basepath + '/preferences'),
      setNamespacePreference: myHelpers.getConfig('PUT', 'REQUEST', basepath + '/preferences'),
      getAppPreference: myHelpers.getConfig('GET', 'REQUEST', basepath + '/apps/:appId/preferences'),
      setAppPreference: myHelpers.getConfig('PUT', 'REQUEST', basepath + '/apps/:appId/preferences'),
      getProgramPreference: myHelpers.getConfig('GET', 'REQUEST', basepath + '/apps/:appId/:programType/:programId/preferences'),
      setProgramPreference: myHelpers.getConfig('PUT', 'REQUEST', basepath + '/apps/:appId/:programType/:programId/preferences')
    });
  });
