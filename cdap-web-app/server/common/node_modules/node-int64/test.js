var assert = require('assert');
var Int64 = require('./Int64');

var args = [
  [0],                     '0000000000000000', 0,
  [1],                     '0000000000000001', 1,
  [-1],                    'ffffffffffffffff', -1,
  [1e18],                  '0de0b6b3a7640000', 1e18,
  ['0001234500654321'],    '0001234500654321',     0x1234500654321,
  ['0ff1234500654321'],    '0ff1234500654321',   0xff1234500654300, // Imprecise!
  [0xff12345, 0x654321],   '0ff1234500654321',   0xff1234500654300, // Imprecise!
  [0xfffaffff, 0xfffff700],'fffafffffffff700',    -0x5000000000900,
  [0xafffffff, 0xfffff700],'affffffffffff700', -0x5000000000000800, // Imprecise!
  ['0x0000123450654321'],  '0000123450654321',      0x123450654321,
  ['0xFFFFFFFFFFFFFFFF'],  'ffffffffffffffff', -1
];

// Test constructor argments

for (var i = 0; i < args.length; i += 3) {
  var a = args[i], octets = args[i+1], number = args[i+2];
  console.log('Testing ' + a.join(', '));
  // Create instance
  var x = new Int64();
  Int64.apply(x, a);

  assert.equal(x.toOctetString(), octets,
               'Constuctor with ' + args.join(', '));

  assert.equal(x.toNumber(true), number);
}
