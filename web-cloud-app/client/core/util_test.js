/**
 * Tests for util.
 */

define(['core/util'], function(util) {

  describe('Given util module it', function() {

    it('should parse query string', function() {
      var path = 'http://localhost:8000/metrics/busyness?id=1&name=2';
      var result = util.parseQueryString(path);
      expect(result).toEqual(jasmine.any(Object));
      expect(result).toEqual({name: '2', id: '1'});
      path = 'http://localhost:8000/metrics/busyness/';
      result = util.parseQueryString(path);
      expect(result).toEqual({});
    });

    it('should have upload and sub processes defined', function() {
      expect(util.Upload).toBeDefined();
      expect(util.Upload.__sendFile).toBeDefined();
      expect(util.Upload.sendFiles).toBeDefined();
      expect(util.Upload.update).toBeDefined();
    });

    it('should have sparkline defined', function() {
      expect(util.sparkline).toBeDefined();
    });

    it('should prettify number', function() {
      var number = 100;
      expect(util.numberArrayToString(number)).toEqual('100');
      number = 1000;
      expect(util.numberArrayToString(number)).toEqual('1000');
      number = 1000000;
      expect(util.numberArrayToString(number)).toEqual('1000K');
      number = 51000000;
      expect(util.numberArrayToString(number)).toEqual('51M');
      number = 51000000000;
      expect(util.numberArrayToString(number)).toEqual('51B');
    });

    it('should prettify bytes', function() {
      var bytes = 51024;
      expect(util.bytes(bytes)).toEqual([49.8, 'KB']);
      bytes = 1024;
      expect(util.bytes(bytes)).toEqual([1024, 'B']);
      bytes = 51048576;
      expect(util.bytes(bytes)).toEqual([48.68, 'MB']);
      bytes = 51073741824;
      expect(util.bytes(bytes)).toEqual([47.57, 'GB']);
    });

  });
});