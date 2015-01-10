/**
 * Dashboards' model
 */

angular.module(PKG.name+'.feature.dashboard').factory('myDashboardsModel',
function (Widget, MyDataSource, $timeout) {

  var dSrc = new MyDataSource(),
      API_PATH = '/configuration/dashboards';


  function Dashboard (p) {
    p = p || {};
    angular.extend(
      this,
      {
        id: p.id,
        title: p.title || 'Dashboard',
        columns: []
      }
    );

    if(angular.isArray(p.columns)) {
      angular.forEach(p.columns, function (c) {
        this.push(c.map(function (o) {
          return new Widget(o);
        }));
      }, this.columns);
    }
    else {
      // default is a single empty column
      this.columns.push([]);
    }
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

    if(this.id) { // updating
      dSrc.request(
        {
          method: 'PUT',
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
        (function (result) {
          this.id = result.id;
        }).bind(this)
      );
    }
  };


/* ------------------------------------------------------------------------- */


  function Model () {
    var data = [];
    data.activeIndex = 0;

    this.data = data;

    dSrc.request(
      {
        method: 'GET',
        _cdapNsPath: API_PATH
      },
      (function (result) {

        if(result.length) {
          // recreate saved dashboards
          angular.forEach(result, function (v) {
            var p = v.config;
            p.id = v.id;
            data.push(new Dashboard(p));
          });

        } else { // no dashboards yet
          this.add(); // create a new default one
        }
      }).bind(this)
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
    // FIXME: figure out why asSortable throws an error without $timeout
    $timeout(function () {
      d.columns[0].push(new Widget());

      // save to backend
      d.persist();
    });

    // newly created tab becomes active
    var n = this.data.push(d);
    this.data.activeIndex = n-1;
  };



  return new Model();
});


