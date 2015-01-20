angular.module(PKG.name + '.services')
  .factory('myCdapUrl', function myCdapUrl($window, $stateParams, $log, MY_CONFIG) {

    function constructUrl(resource) {

      var url;

      if(resource._cdapNsPath) {

        var namespace = $stateParams.namespace;

        if(!namespace) {
          throw new Error('_cdapNsPath requires $stateParams.namespace to be defined');
        }

        resource._cdapPath = [
          '/namespaces/',
          namespace,
          resource._cdapNsPath
        ].join('');
        delete resource._cdapNsPath;
      }

      // further sugar for building absolute url
      if(resource._cdapPath || resource._cdapPathV2) {
        url = [
          'http://',
          MY_CONFIG.cdap.routerServerUrl,
          ':',
          MY_CONFIG.cdap.routerServerPort,
          resource._cdapPathV2 ? '/v2' : '/v3',
          resource._cdapPath || resource._cdapPathV2
        ].join('');

        delete resource._cdapPath;
        delete resource._cdapPathV2;
      }


      return url;
    }

    return {
      constructUrl: constructUrl
    };
  });
