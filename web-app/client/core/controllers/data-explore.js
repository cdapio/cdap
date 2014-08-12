/*
 * Dataexplore Controller
 */

define([], function () {
  var url = 'data/explore/queries';
	var Controller = Em.Controller.extend({

    bindTooltips: function() {
      setTimeout(function () {
        $("[data-toggle='tooltip']").tooltip();
        $("[data-toggle='popover']").popover();
      }, 500)
    },

    contentDidChange: function() {
      this.clearAllTooltips();
      this.bindTooltips();
    }.observes('page'),

    renderExecuteBtn: function() {
      return true;
      return this.get('page') == 'query';
    }.property('page'),

		load: function () {
		  var self = this;

      //largestEver is the largest timestamp of a query that the controller has seen.
      //largest is the largest timestamp of a query on the current page.
      //smallestEver is the smallest timestamp of a query that the controller has seen.
      //smallest is the largest timestamp of a query on the current page.
		  self.pageMgr = {
        limit: 3,
        largestEver: 0,
        smallestEver: Infinity,
        firstPage: function () {
          this.offset = null;
          this.cursor = null;
        }
		  };
      self.pageMgr.firstPage();

		  this.set('objArr', []);
		  this.fetchQueries();
		  this.interval = setInterval(function () {
        self.fetchQueries();
		  }, 5000);
		  this.set('datasets', []);
		  this.loadDiscoverableDatasets();
		},

    clearAllTooltips: function () {
      $("[data-toggle='tooltip']").tooltip('hide');
      $("[data-toggle='popover']").popover('hide');
    },

		loadDiscoverableDatasets: function () {
		  var self = this;
      var datasets = self.get('datasets');
		  self.HTTP.rest('data/datasets?meta=true&explorable=true', function (response) {
		    response.forEach(function (dataset) {
		      var name = dataset.hive_table;
          var shortName = dataset.spec.name.replace(/.*\./,'');
          self.HTTP.rest('data/explore/datasets/' + shortName + '/schema', function (response, status) {
            var results = [];
            for(var key in response) {
              if(response.hasOwnProperty(key)){
                results.push({columns:[key, response[key]]});
              }
            }
            datasets.pushObject(Ember.Object.create({name:name, shortName:shortName, results:results}));

            if(datasets.length == 1){
              self.selectDataset(datasets[0]);
            }
          });
		    });
		  });
		},

    tableClicked: function (obj) {
      this.injectorTextArea.set('value', obj.statement);
      this.toggleTable(obj);
    },

		toggleTable: function (obj, hideAllFirst /* = true */, callback) {
      var self = this;
      hideAllFirst = typeof hideAllFirst !== 'undefined' ? hideAllFirst : true;
      if(hideAllFirst){
        self.hideAllTables(obj);
      }
      if(!obj.get('preview_cached') || !obj.get('has_results') || !obj.get('is_active')) {
        setTimeout(function(){
          $("[id='#" + obj.get('query_handle') + "']").tooltip('show');
          setTimeout(function(){
            $("[id='#" + obj.get('query_handle') + "']").tooltip('hide');
          }, 1000);
        }, 200)
        return;
      }
      obj.set('isSelected', !obj.get('isSelected'));
      $("#" + obj.query_handle).slideToggle(200, function () {
        var objArr = self.get('objArr');
        objArr.forEach(function (entry) {
          if (!$("#" + obj.query_handle + " :visible")) {
            entry.set('isSelected', false);  
          }
        });
        if (typeof callback === "function") {
            callback();
        }
      });
    },

    hideAllTables: function (dontHide) {
      var objArr = this.get('objArr');
      for (var i=0; i<objArr.length; i++){
        var query = objArr[i];
        if(query === dontHide){
          continue;
        }
        if(query.get('isSelected')){
          this.toggleTable(query, false);
          query.set('isSelected', false);
        }
      }
    },

    hideTable: function (query, callback) {
      if(query.get('isSelected')){
        this.toggleTable(query, false, callback);
      } else if (typeof callback === "function") {
        callback();
      }
    },

    selectDataset: function (dataset) {
      this.set('selectedDataset', dataset);
      this.injectorTextArea.set('value', 'SELECT * FROM ' + dataset.name + ' LIMIT 5');
      var datasets = this.get('datasets');
      datasets.forEach(function (entry) {
        entry.set('isSelected', false);
      });
      dataset.set('isSelected', true);
    },

		unload: function () {},

    nextPage: function () {
      this.pageMgr.offset = this.smallest;
      this.pageMgr.cursor = "next";
      this.fetchQueries(true);
    },

    prevPage: function () {
      this.pageMgr.offset = this.largest;
      this.pageMgr.cursor = "prev";
      this.fetchQueries(true);
    },

    pageNavTooltip: function () {
      var className = this.pageMgr.cursor + "Btn";
      $("." + className).tooltip('show');
      setTimeout(function(){
        $("." + className).tooltip('hide');
      }, 2000);
    },

		fetchQueries: function (userAction) {
		  var self = this;

      if(self.largest == self.pageMgr.largestEver){
        //If the user is trying to go to a previous page when already viewing the most recent query:
        if(self.pageMgr.cursor === "prev" && userAction){
          self.pageNavTooltip();
        }
        if(!userAction){
          self.pageMgr.firstPage();
        }
      }

		  var url = 'data/explore/queries';
      url += '?limit=' + self.pageMgr.limit;
		  if(self.pageMgr.offset){
		    url += '&offset=' + self.pageMgr.offset;
		  }
		  if(self.pageMgr.cursor){
        url += '&cursor=' + self.pageMgr.cursor;
      }
      this.HTTP.rest(url, function (queries, status) {
        if(queries.length == 0){
          if(userAction){
            //When trying to go to next page, and there are no results there:
            self.pageNavTooltip();
          }
          return;
        }

        //Clear the local memory of the queries if the list of queries in the response is different than the local version (and our local version isn't empty)
        if(userAction || (  self.get('objArr').length && (self.get('objArr')[0].get('query_handle') != queries[0].query_handle)  )){
          self.set('objArr', []);
        }
		    var objArr = self.get('objArr');

        queries.forEach(function (query) {
          var existingObj = self.find(query.query_handle);
          if (!existingObj) {
            var newObj = Ember.Object.create(query);
            newObj.query_handle_hashed = "#" + newObj.query_handle;
            newObj.time_started = new Date(query.timestamp);
            newObj.time_started = newObj.time_started.toLocaleString();

            existingObj = objArr.pushObject(newObj);
          } else if (   existingObj.get('status')      !== query.status
                     || existingObj.get('has_results') !== query.has_results
                     || existingObj.get('is_active')   !== query.is_active) {
              //Avoid updating the local query object upon every response from the server.
              //Instead, only update the query object when attributes actually change.
              //Otherwise, emberjs will update the view (even when no attribute really changed) and the update can mess up any dynamic view (toggled tables).
              self.hideTable(existingObj, function () {
                existingObj.set('status', query.status);
                existingObj.set('has_results', query.has_results);
                existingObj.set('is_active', query.is_active);
              });
          }
          if ( existingObj.get('status') === 'FINISHED'
               && existingObj.get('has_results')
               && existingObj.get('is_active')
               && !existingObj.get('preview_cached') ) {
            self.getPreview(existingObj);
            self.getSchema(existingObj);
          }
        });


        if(objArr.length){
          //keep track of largest and smallest timestamps for the purposes of page navigation.
          self.largest = objArr[0].timestamp;
          self.smallest = objArr[objArr.length - 1].timestamp;
          if(self.largest > self.pageMgr.largestEver) {
            self.pageMgr.largestEver = self.largest;
          }
          if(self.smallest < self.pageMgr.smallestEver) {
            self.pageMgr.smallestEver = self.smallest;
          }
        }
        self.bindTooltips();
      });
		},

    getPreview: function (query) {
      var self = this;
      var handle = query.get('query_handle');
      this.HTTP.post('rest/data/explore/queries/' + handle + '/preview', function (response) {
        response = jQuery.parseJSON(response);
        if(response.length == 0){
          query.set('is_active', false);
        }
        query.set('preview_cached', response);
      });
    },

    downloadFile: function(filename, content) {
      var blob = new Blob([content]);
      var evt = document.createEvent("HTMLEvents");
      evt.initEvent("click");
      $("<a>", {
        download: filename,
        href: webkitURL.createObjectURL(blob)
      }).get(0).dispatchEvent(evt);
    },

    //TODO: make this functionality compatible with more browsers.
    download: function (query) {
      var self = this;
      var handle = query.get('query_handle');
      var url = 'rest/data/explore/queries/' + handle + '/download';

      var handle = query.get('query_handle');
      self.HTTP.post(url, function (response) {
        self.downloadFile('results_' + handle + '.txt', response);
      });
      this.hideTable(query, function () {
        query.set('is_active', false);
      });
    },

    deleteQuery: function (query) {
      var handle = query.get('query_handle');
      this.HTTP.del('rest/data/explore/queries/' + handle);
    },

    getSchema: function (query) {
      var self = this;
      var handle = query.get('query_handle');
      this.HTTP.rest('data/explore/queries/' + handle + '/schema', function (response) {
        query.set('schema', response);
      });
    },

    submitSQLQuery: function () {
      var self = this;
      var controller = this.get('controllers');
      var sqlString = controller.get("SQLQueryString") || this.injectorTextArea.get('value');
      this.HTTP.post('rest/data/explore/queries', {data: { "query": sqlString }},
        function (response, status) {
          if(status != 200) {
            $(".text-area-container").attr('data-content', response.message).popover('show')
            .click(function(){},function(){$(".text-area-container").popover('hide')});
            return;
          }
          self.transitionToRoute('DataExplore.Results');
          self.pageMgr.firstPage();
          self.fetchQueries();
          self.pageMgr.largestEver += 1;
        }
      );
      self.hideAllTables();
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

	});

	Controller.reopenClass({
		type: 'DataExplore',
		kind: 'Controller'
	});

	return Controller;

});
