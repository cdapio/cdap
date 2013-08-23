/**
 * Tests for application.
 */

define(['core/application'], function(App) {

  describe('Given application module it', function() {
    var app = App.create();

    beforeEach(function() {
      app.reset();
    });

    it('should have defaults defined', function() {
      expect(app.ENTITY_MAP['FLOW']).toBeDefined();
      expect(app.LOG_TRANSITIONS).toBeDefined();
      expect(app.POLLING_INTERVAL).toBeDefined();
      expect(app.EMBEDDABLE_DELAY).toBeDefined();
      expect(app.WATCH_LATENCY).toBeDefined();
    });

    it('should have element defined', function() {
      expect(app.ApplicationView.create().get('elementId')).toEqual('content');
    });

    it('should show message if version is not current', function() {
      var controller = app.ApplicationController.create();
      var version = {'newest': 1, 'current': 1};
      controller.checkVersion(version);

      // TODO: make test reliable.
      expect($('#warning').text()).toNotEqual(
        'New version available: 1 » 1 Click here to download.');
      
      version = {'newest': 1, 'current': 1.5};
      controller.checkVersion(version);
      expect($('#warning').text()).toEqual(
        'New version available: 1.5 » 1 Click here to download.');
    });

    it('should set up environment correctly', function() {
      var env = {product: "Local", version: "developer", credential: null};
      app.setupEnvironment(env);
      expect(C.Env.get('version')).toEqual('developer');
      expect(C.Env.get('productName')).toEqual('local');
      expect(C.Env.get('credential')).toEqual('');
    });

    it('should set time range', function() {
      millis = 3600;
      app.setTimeRange(millis);
      expect(app.get('__timeRange')).toEqual(millis);
      expect(app.get('__timeLabel')).toEqual('Last 1 Hour');
      millis = 600;
      app.setTimeRange(millis);
      expect(app.get('__timeRange')).toEqual(millis);
      expect(app.get('__timeLabel')).toEqual('Last 10 Minutes');
      millis = 60;
      app.setTimeRange(millis);
      expect(app.get('__timeRange')).toEqual(millis);
      expect(app.get('__timeLabel')).toEqual('Last 1 Minute');
    });

    it('should dynamically fetch theme', function() {
      var env = {product: "Local", version: "developer", credential: null};
      app.setupEnvironment(env);
      var themeLink = app.getThemeLink();
      var url = window.location.href;
      var ids = url.split('/');
      var hostAndPort = ids[0] + '//' + ids[2];
      expect(themeLink.href).toEqual(hostAndPort + '/assets/css/local.css');
    });

  });
});