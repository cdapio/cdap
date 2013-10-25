
/*
 * Main entry point for Reactor UI
 * Defines routes and attaches mocks
 */

var searchLoad = false;

define (['js/components'],
  function (Components, Util) {

  window.SplunkLite = window.S = Em.Application.create({

    ApplicationController: Ember.Controller.extend({

      updateCurrentPath: function() {
        S.set('currentPath', this.get('currentPath'));
      }.observes('currentPath')

    })
  });

  Components.forEach(function (component) {
    S[component.kind === 'Model' ?
      component.type : component.type + component.kind] = component;
  });

  S.Router.map(function() {
    this.resource('Index', { path: '/' });
    this.resource('Log', { path: '/log' });
    this.resource('Search', { path: '/search' });
    this.resource('Trend', { path: '/trend' });
    this.resource('Alerts', { path: '/alerts' });
    this.route('PageNotFound', { path: '*:'});
  });

  S.Router.reopen({
    location: 'none'
  });

  var basicRouter = Ember.Route.extend({
    setupController: function(controller, model) {
    
    
      controller.set('model', model);
      controller.load(this.routeName);
    },
    deactivate: function () {
      this.controller.unload();
    },
  });

  $.extend(S, {
    IndexRoute: Ember.Route.extend({
      redirect: function() {
        this.transitionTo('Alerts');
      }
    }),
    WelcomeRoute: basicRouter.extend(),
    LogRoute: basicRouter.extend(),
    SearchRoute: basicRouter.extend(),
    TrendRoute: basicRouter.extend(),
    AlertsRoute: basicRouter.extend(),
  });
  
  $.getJSON('/trend?level=ERROR', function(errors) {
      var errorData = [];
      var timestamps = [];
  
      for (var i = 0; i < errors.length; i ++) {

         errorData.push(errors[i].count);
         timestamps.push(errors[i].timestamp);

       }

      $.getJSON('/trend', function (trends) {
          var warnData = [];
          
          for (var i = 0; i < trends.length; i ++) {

            warnData.push(trends[i].count);
         timestamps.push(trends[i].timestamp);

          }
        timestamps = sort_unique(timestamps);

          $('#bar').highcharts({
              chart: {
                  type: 'column'
              },
              title: {
                  text: 'Log Trend'
              },
              xAxis: {
                  categories: timestamps,
                  labels: {
                    formatter: function(){
                      return new Date(this.value).toLocaleString();
                    }
                }
              },
              yAxis: {
                  min: 0,
                  title: {
                      text: 'Count'
                  },
                  allowDecimals: false,
              },
              legend: {
                  backgroundColor: '#FFFFFF',
                  reversed: true
              },
              plotOptions: {
                  series: {
                      stacking: 'normal',
		              cursor: 'pointer',
                      point: {
                      events: {
                        click: function() {
                            var searchController = S.__container__.lookup('controller:Search');
                            console.log(this);
                            searchController.set('params', {start: this.category, end: this.category + 60*60*1000, level: this.series.name});
                            
                            S.__container__.lookup('controller:application').transitionToRoute('Search');
                            
                            if (searchLoad) {
                              searchController.load();
                            }
                            searchLoad = true;
                        }
                      }
                    }
                  }
              },
              series: [{
                name: 'ERROR',
                data: errorData,
                stack: 0,
                color: '#FF0000',
                pointWidth: 28
              }, {
                name: 'WARN',
                data: warnData,
                stack: 1,
                pointWidth: 28
              }]
          });

	  });
	
	});

function sort_unique(arr) {
    arr = arr.sort(function (a, b) { return a*1 - b*1; });
    var ret = [arr[0]];
    for (var i = 1; i < arr.length; i++) { // start loop at 1 as element 0 can never be a duplicate
        if (arr[i-1] !== arr[i]) {
            ret.push(arr[i]);
        }
    }
    return ret;
}
});
