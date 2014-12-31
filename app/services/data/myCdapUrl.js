angular.module(PKG.name + '.services')
  .factory('myCdapUrl', function myCdapUrl($state, MY_CONFIG) {
    var baseUrl = [
      'http://',
      MY_CONFIG.cdap.routerServerUrl,
      ':',
      MY_CONFIG.cdap.routerServerPort,
      '/v3'
    ].join('');

    function constructUrl(resource) {
      var url;
      if(resource._cdapNsPath) {
        resource._cdapPath = [
          '/namespaces/',
          $state.params.namespaceId,
          resource._cdapNsPath
        ].join('');
        delete resource._cdapNsPath;
      }

      // further sugar for building absolute url
      if(resource._cdapPath) {
        url = [
          baseUrl,
          resource._cdapPath
        ].join('');
        delete resource._cdapPath;
      }
      return url;
    }

    return {
      constructUrl: constructUrl
    };
  });
