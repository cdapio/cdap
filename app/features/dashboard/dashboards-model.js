/**
 * Dashboards' model
 */

angular.module(PKG.name+'.feature.dashboard').factory('myDashboardsModel',
function (Widget, MyDataSource) {

  var data = new MyDataSource(),
      API_PATH = '/configuration/dashboards';


  function Dashboard (title, colCount) {
    var c = [[]];
    for (var i = 1; i < (colCount||3); i++) {
      c.push([]);
    }

    c[0].push(new Widget());

    this.title = title || 'Dashboard';
    this.columns = c;
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

    this.columns[index].unshift(w || new Widget({title: 'just added'}));
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
   * save to backend
   */
  Dashboard.prototype.persist = function () {
    if(this.id) { // updating
      data.request(
        {
          method: 'POST',
          _cdapNsPath: API_PATH + '/' + this.id,
          body: this
        }
      );
    }
    else { // saving
      data.request(
        {
          method: 'POST',
          _cdapNsPath: API_PATH,
          body: this
        },
        angular.bind(this, function (result) {
          this.id = result.id || result;
        })
      );
    }
  };


/* ------------------------------------------------------------------------- */


  function Model () {
    this.data = [];
    this.data.activeIndex = 0;

    data.request(
      {
        method: 'GET',
        _cdapNsPath: API_PATH
      },
      angular.bind(this, function (result) {
        console.log(result);
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
    this.data.splice(index, 1);
  };


  /**
   * add a new dashboard tab
   */
  Model.prototype.add = function (title, colCount) {
    var d = new Dashboard(title, colCount);
    d.persist();

    var n = this.data.push(d);
    this.data.activeIndex = n-1;
  };



  return new Model();
});


