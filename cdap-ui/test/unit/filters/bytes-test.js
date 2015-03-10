'use strict';

describe('bytes', function() {

  beforeEach(module('cdap-ui.filters'));

  var bytes;

  beforeEach(inject(function(_$filter_) {
    bytes = _$filter_('bytes');
  }));


  it('should convert bytes to kB', function() {
    var b = 1024;

    expect(bytes(b)).toBe('1.0kB');
  });

  it('should convert bytes to MB', function() {
    var b = 1048576;

    expect(bytes(b)).toBe('1.0MB');
  });

  it('should convert bytes to GB', function() {
    var b = 1073741824;

    expect(bytes(b)).toBe('1.0GB');
  });

  it('should convert bytes to TB', function() {
    var b = 1099511627776;

    expect(bytes(b)).toBe('1.0TB');
  });

  it('should convert bytes to PB', function() {
    var b = 1099511627776 * 1024;

    expect(bytes(b)).toBe('1.0PB');
  });

  it('should return 0 bytes when input is not a number', function() {

    var input = 'str';
    expect(bytes(input)).toBe('0b');

    input = [1, 2, 3];
    expect(bytes(input)).toBe('0b');

    input = { 1: 1, 2: 2};
    expect(bytes(input)).toBe('0b');

    input = null;
    expect(bytes(input)).toBe('0b');

    input = undefined;
    expect(bytes(input)).toBe('0b');
  });

});