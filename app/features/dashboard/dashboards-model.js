/**
 * Dashboards' model
 */

angular.module(PKG.name+'.feature.dashboard').factory('myDashboardsModel',
function (Widget, MyDataSource, $log) {

  var dSrc = new MyDataSource(),
      API_PATH = '/configuration/dashboards';


  function Dashboard (properties) {
    angular.extend(
      this,
      {
        title: 'Dashboard',
        columns: [[]]
      },
      properties || {}
    );
  }


  /**
   * rename a widget
   */
  Dashboard.prototype.renameWidget = function (widget, newName) {
    if(newName) {
      widget.title = newName;
      this.persist();
    }
  };


  /**
   * remove a widget from the active dashboard tab
   * @param  {object} w the widget object
   */
  Dashboard.prototype.removeWidget = function (widget) {
    var that = this;
    angular.forEach(that.columns, function (c, i) {
      that.columns[i] = c.filter(function (p) {
        return widget !== p;
      });
    });
    this.persist();
  };



  /**
   * add a widget to the active dashboard tab
   */
  Dashboard.prototype.addWidget = function (w) {
    var c = this.columns,
        index = 0,
        smallest;

    // find the column with the least widgets
    for (var i = 0; i < c.length; i++) {
      var len = c[i].length;
      if(smallest===undefined || (len < smallest)) {
        smallest = len;
        index = i;
      }
    }

    w = w || new Widget({
      title: 'just added'
    });

    this.columns[index].unshift(w);
    this.persist();
  };



  /**
   * rename dashboard tab
   */
  Dashboard.prototype.rename = function (newName) {
    if(newName) {
      this.title = newName;
      this.persist();
    }
  };



  /**
   * benefit from angular's toJsonReplacer
   */
  Dashboard.prototype.properties = function () {
    return angular.fromJson(angular.toJson(this));
  };


  /**
   * save to backend
   */
  Dashboard.prototype.persist = function () {
    var body = this.properties();

    $log.log('persist', body);

    if(this.id) { // updating
      dSrc.request(
        {
          method: 'POST',
          _cdapNsPath: API_PATH + '/' + this.id,
          body: body
        }
      );
    }
    else { // saving
      dSrc.request(
        {
          method: 'POST',
          _cdapNsPath: API_PATH,
          body: body
        },
        angular.bind(this, function (result) {
          // FIXME: result format in flux
          this.id = result.id || result;
        })
      );
    }
  };


/* ------------------------------------------------------------------------- */


  function Model () {
    this.data = [];
    this.data.activeIndex = 0;

    dSrc.request(
      {
        method: 'GET',
        _cdapNsPath: API_PATH
      },
      angular.bind(this, function (result) {

        $log.log('dashboard model', result);

        angular.forEach(result, function (v, k) {

          // FIXME: API should not returned nested strings of JSON!
          var properties = JSON.parse(v);
          properties.id = k;

          this.push(new Dashboard(properties));

        }, this.data);
      })
    );

  }


  /**
   * direct access to the current tab's Dashboard instance
   */
  Model.prototype.current = function () {
    return this.data[this.data.activeIndex || 0];
  };



  /**
   * remove a dashboard tab
   */
  Model.prototype.remove = function (index) {
    var removed = this.data.splice(index, 1)[0];

    dSrc.request(
      {
        method: 'DELETE',
        _cdapNsPath: API_PATH + '/' + removed.id
      },
      function () {
        $log.log('removed', removed);
      }
    );
  };


  /**
   * add a new dashboard tab
   */
  Model.prototype.add = function (properties, colCount) {
    var d = new Dashboard(properties);

    // add columns as needed
    for (var i = 1; i < (colCount||3); i++) {
      d.columns.push([]);
    }

    // default widget in first column
    d.columns[0].push(new Widget());

    // save to backend
    d.persist();

    // newly created tab becomes active
    var n = this.data.push(d);
    this.data.activeIndex = n-1;
  };



  return new Model();
});


