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
		  self.getExploreStatus();
		  var objArr = this.get('objArr');
      this.HTTP.rest('data/explore/queries', function (queries, status) {
        if(status != 200) {
          console.log('error in fetchQueries in data-explore.js');
          return;
        }
        queries.forEach(function (query) {
          var existingObj = self.find(query.query_handle);
          if (!existingObj) {
            var newObj = Ember.Object.create(query);
            newObj.query_handle_hashed = "#" + newObj.query_handle;
            existingObj = objArr.pushObject(newObj);
          } else {
            existingObj.set('status', query.status);
            existingObj.set('has_results', query.has_results);
            existingObj.set('is_active', query.is_active);
          }

          if (existingObj.get('has_results')) {
            if (!existingObj.get('results')) {
              self.getResults(existingObj);
              self.getSchema(existingObj);
            }
          }
        });
      });
		},

    getExploreStatus: function () {
      var self = this;
      this.HTTP.rest('explore/status', function () {
//        console.log(response + "s");
      });
    },

    getResults: function (query) {
      var self = this;
      var handle = query.get('query_handle');
      this.HTTP.post('rest/data/queries/' + handle + '/next', function (response) {
        query.set('results', response);
        query.set('downloadableResults', "data:text/plain;charset=UTF-8," + response);
        query.set('downloadName', "results_" + handle + ".txt");
      });
    },

    getSchema: function (query) {
      var self = this;
      var handle = query.get('query_handle');
      this.HTTP.rest('data/queries/' + handle + '/schema', function (response) {
        query.set('schema', response);
        console.log(response);
      });
    },

    submitSQLQuery: function () {
      var controller = this.get('controllers');
      var sqlString = controller.get("SQLQueryString");
      this.HTTP.post('rest/data/queries', {data: { "query": sqlString }},
        function (response, status) {
          if(status != 200) {
            console.log('error in submitSQLQuery in data-explore.js');
            return;
          }
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


    //TODO
    //HTTP.rest('data/explore/queries',function(response){});
    //HTTP.rest('data/explore/queries/{handleID}/schema'

	});

	Controller.reopenClass({
		type: 'DataExplore',
		kind: 'Controller'
	});

	return Controller;

});
