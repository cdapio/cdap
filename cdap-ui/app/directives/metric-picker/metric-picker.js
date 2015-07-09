angular.module(PKG.name + '.commons')
  .directive('myMetricPicker', function (MyDataSource, $stateParams, $log, MyMetricsQueryHelper) {

    var dSrc = new MyDataSource();

    function MetricPickerCtrl ($scope) {

      var ns = [$stateParams.namespace,'namespace'].join(' ');

      $scope.available = {
        contexts: [],
        types: ['system', ns],
        names: []
      };

      $scope.metric = {
        context: '',
        type: ns,
        names: [{name: ''}],
        resetNames: function() {
          this.names = [{name: ''}];
        },
        getNames: function() {
          return this.names.map(function(value) {
            return value.name;
          });
        },
        getName: function() {
          return this.getNames().join(', ');
        }
      };

      $scope.addMetricName = function() {
        $scope.metric.names.push({name: ''});
      };

      $scope.deleteMetric = function(idx) {
        if ($scope.metric.names.length === 1) {
          // If its the only metric, simply clear it instead of removing it
          $scope.metric.resetNames();
        } else {
          $scope.metric.names.splice(idx, 1);
        }
      };

    }

    return {
      restrict: 'E',

      require: 'ngModel',

      scope: {},

      templateUrl: 'metric-picker/metric-picker.html',

      controller: MetricPickerCtrl,

      link: function (scope, elem, attr, ngModel) {

        if(attr.required!==undefined) {
          elem.find('input').attr('required', true);
          // TODO: if the validators fail, propagate this information to the user
          ngModel.$validators.metricAndContext = function (m, v) {
            var t = m || v;
            // Metrics context requires an even number of parts
            if (t.context && t.context.split('.').length % 2 !== 0) {
              return false;
            }
            if (!t || !t.names || !t.names.length) {
              return false;
            }
            for (var i = 0; i < t.names.length; i++) {
              if (!t.names[i].length) {
                return false;
              }
            }
            return true;
          };
        }

        function getBaseContext () {
          var output;

          if(scope.metric.type==='system') {
            output = 'system';
          }
          else {
            output = $stateParams.namespace;

            if(!output) { // should never happen, except on directive playground
              output = 'default';
              $log.warn('metric-picker using default namespace as context');
            }

          }

          return 'namespace.' + output;
        }

        function fetchAhead () {
          var context = getBaseContext(),
              bLen = context.length + 1;

          if(scope.metric.context) {
            context += '.' + scope.metric.context;
          }
          var parts = context.split('.');
          if (parts.length % 2 !== 0) {
            return;
          }

          var tagQueryParams = MyMetricsQueryHelper.tagsToParams(MyMetricsQueryHelper.contextToTags(context));
          scope.available.contexts = [];
          dSrc.request(
            {
              method: 'POST',
              _cdapPath: '/metrics/search?target=tag&' + tagQueryParams
            },
            function (res) {
              res = res.map(function(v) {
                return context + '.' +  MyMetricsQueryHelper.tagToContext(v);
              });
              scope.available.contexts = res.map(function(d){
                return {
                  value: d.substring(bLen),
                  display: d.substring(bLen+scope.metric.context.length)
                };
              }).filter(function(d) {
                return d.display;
              });
            }
          );

          scope.available.names = [];
          dSrc.request(
            {
              method: 'POST',
              _cdapPath: '/metrics/search?target=metric&' + tagQueryParams
            },
            function (res) {
              // 'Add All' option to add all metrics in current context.
              res.unshift('Add All');
              scope.available.names = res;
            }
          );

        }

        var onBlurHandler = ngModel.$setTouched.bind(ngModel);
        elem.find('button').on('blur', onBlurHandler);
        elem.find('input').on('blur', onBlurHandler);

        var metricChanged = function (newVal, oldVal) {
          ngModel.$validate();

          if(newVal.type !== oldVal.type) {
            scope.metric.context = '';
            scope.metric.resetNames();
            fetchAhead();
            return;
          }

          if(newVal.context !== oldVal.context) {
            scope.metric.resetNames();
            fetchAhead();
            return;
          }

          if(newVal.names) {
            var isAddAll = false;
            for (var i = 0; i < newVal.names.length; i++) {
              if (newVal.names[i].name === 'Add All') {
                isAddAll = true;
              }
            }
            var context = getBaseContext();
            if (newVal.context) {
              context += '.' + newVal.context;
            }
            if (isAddAll) {
              ngModel.$setViewValue({
                addAll: true,
                allMetrics: scope.available.names.slice(1), // Remove 'Add All' option
                context: context,
                names: newVal.getNames(),
                name: newVal.getName()
              });
              return;
            } else {
              ngModel.$setViewValue({
                context: context,
                names: newVal.getNames(),
                name: newVal.getName()
              });
            }
          } else {
            if(ngModel.$dirty) {
              ngModel.$setViewValue(null);
            }

          }

        };
        scope.$watch('metric', metricChanged, true);

        fetchAhead();
      }
    };
  });
