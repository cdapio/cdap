/*
 * Dataexplore Controller
 */

define([], function () {
  var url = "data/explore/queries";
	var Controller = Em.Controller.extend({


		load: function () {
		  var self = this;
		  this.set('objArr', []);
		  this.fetchQueries();
		  this.interval = setInterval(function () {
        self.fetchQueries();
		  }, 1000);
		},


		unload: function () {
		},

		fetchQueries: function () {
		  var self = this;
		  var objArr = this.get('objArr');
      this.HTTP.rest('data/explore/queries', function (queries, status) {
        if(status != 200) {
          console.log('error in fetchQueries in data-explore.js');
          return;
        }
        queries.forEach(function (query) {
          var existingObj = self.find(query.query_handle);
          if (!existingObj) {
            objArr.pushObject(Ember.Object.create(query));
          } else {
//            existingObj.set('status', query.status);
            existingObj.set('has_results', query.has_results);
            existingObj.set('is_active', query.is_active);
            existingObj.set('status', existingObj.get('status')==="FINISHED"?"RUNNING":"FINISHED");
          }
        });
      });
		},

    submitSQLQuery: function () {
      var sqlString = this.get("SQLQueryString");
      this.HTTP.post('rest', 'data/explore/queries', { "query": sqlString },
        function (response) {
          //TODO: do something with the handle, such as adding it to list of queries. or refresh the list.
        }
      );
    },

    find: function (query_handle) {
      var objArr = this.get('objArr');
      for (var i=0; i<objArr.length; i++){
        if (query_handle === objArr[i].query_handle) {
          return objArr[i];
        }
      }
      return false;
    },

    //TODO: loop over all queries, and update their status every interval=2000ms?
    //HTTP.rest('data/explore/queries/{handleID}/status');
    //HTTP.rest('data/explore/queries');

    //TODO
    //HTTP.rest('explore/status',function(response){});
    //HTTP.rest('data/explore/queries',function(response){});
    //HTTP.rest('data/explore/queries/{handleID}/schema'

	});

	Controller.reopenClass({
		type: 'DataExplore',
		kind: 'Controller'
	});

	return Controller;

});
