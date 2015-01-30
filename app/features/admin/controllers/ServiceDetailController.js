angular.module(PKG.name + '.feature.admin')
  .controller('AdminServiceDetailController', function ($scope, $state) {
    $scope.service = {
        name: $state.params.serviceName,
        description: 'sehiueoowriugn weogiunwg woginwgowg woginwg owign wgoiwg owign wg oiwng woieg',
        zookeeper: 'string',
        hostPort: [
        {
            host: 'abc',
            port: 'xyz'
        },
        {
            host: 'def',
            port: 'uvw'
        }
        ]
    };
  });
