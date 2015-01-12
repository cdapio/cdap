angular.module(PKG.name + '.commons')
  .directive('myMetricPicker', function (MyDataSource) {

    var dSrc = new MyDataSource();

    function MetricPickerCtrl ($scope) {

      var a = ['system', 'user'];

      $scope.available = {
        types: a,
        contexts: [],
        metrics: []
      };

      $scope.metric = {
        type: a[0],
        context: '',
        name: ''
      };

    }

    return {
      restrict: 'E',

      require: 'ngModel',

      scope: true,

      templateUrl: 'metric-picker/metric-picker.html',

      controller: MetricPickerCtrl,

      link: function (scope, elem, attr, ngModel) {

        function fetchAhead () {
          var v = (scope.metric.context||'').replace(/\.$/, ''),
              u = ['/metrics', scope.metric.type, v].join('/');

          dSrc.request(
            {
              _cdapNsPath: u + '?search=childContext'
            },
            function (r) {
              scope.available.contexts = r.map(function (c) {
                return v ? v + '.' + c : c;
              });
            }
          );

          scope.available.metrics = v ? dSrc.request(
            {
              _cdapNsPath: u + '/metrics'
            }
          ) : [];

        }

        var s = ngModel.$setTouched.bind(ngModel);
        elem.find('button').on('blur', s);
        elem.find('input').on('blur', s);

        scope.$watchCollection('metric', function (newVal, oldVal) {

          ngModel.$validate();

          if(newVal.type !== oldVal.type) {
            scope.metric.context = '';
            scope.metric.name = '';
            fetchAhead();
            return;
          }

          if(newVal.context !== oldVal.context) {
            scope.metric.name = '';
            fetchAhead();
          }

          if(newVal.context && newVal.name) {
            var m = [
              '/metrics',
              newVal.type,
            ];
            if(newVal.context) {
              m.push(newVal.context);
            }
            m.push(newVal.name);

            ngModel.$setViewValue(m.join('/'));
          }
          else {
            if(ngModel.$dirty) {
              ngModel.$setViewValue(null);
            }

          }

        });

        fetchAhead();
      }
    };
  });
