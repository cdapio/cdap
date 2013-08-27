/**
 * Tests for http resource.
 */

define(['core/http'], function(HTTP) {

  describe('Given http resource adapter', function() {

    var http = HTTP.create();
    var args1 = ['metrics', 1, 2, 3, {'type': 'type1', 'count': 'count1'}, function(data) {}];
    var args2 = ['metrics'];
    var args3 = [];

    it('should find path based on arguments', function() {
      var path = http.findPath(args1);
      expect(path).toEqual('/metrics/1/2/3');
      path = http.findPath(args2);
      expect(path).toEqual('/metrics');
      path = http.findPath(args3);
      expect(path).toEqual('/');
    });

    it('should find query string based on arguments', function() {
      var qs = http.findQueryString(args1);
      expect(qs).toEqual('type=type1&count=count1')
      qs = http.findQueryString(args2);
      expect(qs).toEqual('');
      qs = http.findQueryString(args3);
      expect(qs).toEqual('');
    });

    it('should find object based on arguments', function() {
      var obj = http.findObject(args1);
      expect(obj).toEqual(jasmine.any(Object));
      expect(obj).toEqual({ type : 'type1', count : 'count1' });
      obj = http.findObject(args2);
      expect(obj).toEqual();
      obj = http.findObject(args3);
      expect(obj).toEqual();
    });

    it('should find callback based on arguments', function() {
      var fn = http.findCallback(args1);
      expect(fn).toEqual(jasmine.any(Function));
      expect(fn).toEqual(args1[5]);
    });

  });
});