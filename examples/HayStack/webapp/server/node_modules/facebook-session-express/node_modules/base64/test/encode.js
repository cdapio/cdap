var base64 = require('base64');
var crypto = require('crypto');
var fs = require('fs');
var Hash = require('traverse/hash');
var hashes = JSON.parse(
    fs.readFileSync(__dirname + '/hashes.json').toString()
);

function md5hex (data) {
    return new crypto.Hash('md5').update(data.toString()).digest('hex');
}

exports.encode = function (assert) {
    Hash(hashes).forEach(function (hash, file) {
        fs.readFile(__dirname + '/' + file, function (err, buf) {
            if (err) throw err;
            assert.equal(hash, md5hex(base64.encode(buf)));
        });
    });
};

exports.symmetric = function (assert) {
    var buf = new Buffer('pow biff zing');
    assert.equal(
        buf,
        base64.decode(base64.encode(buf))
    );
};
