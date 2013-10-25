
var assert = require('assert');
var path = require('path');
var fs = require('fs');
var cb = require('./cbutil');

function Multipart() {
  this.dash = new Buffer('--', 'ascii');

  this.boundary = this.generateBoundary();

  this.parts = [];
}

Multipart.prototype.dash = null;
Multipart.prototype.boundary = null;
Multipart.prototype.crlf = new Buffer('\r\n', 'ascii');

Multipart.prototype.generateBoundary = function generateBoundary() {
  return new Buffer(
    Math.floor(Math.random() * 0x80000000).toString(36) +
    Math.abs(Math.floor(Math.random() * 0x80000000) ^ +new Date()).toString(36),
    'ascii'
  );
};

Multipart.prototype.addFile = function addFile(name, filePath, callback) {
  var self = this;
  fs.open(filePath, 'r', function(err, fd) {
    if (err) {
      callback(err);
      return;
    }
    try {
      fs.fstat(fd, cb.returnToCallback(callback, false, function(stat) {
        var fileName = path.basename(filePath);
        self.addStream(name, stat.size, fs.createReadStream(filePath, { fd: fd }), null, fileName);
        return null;
      }));
    }
    catch (e) {
      callback(e);
    }
  });
};

Multipart.prototype.addFile = cb.wrap(Multipart.prototype.addFile);

Multipart.prototype.addBuffer = function addBuffer(name, buffer, mime, fileName) {
  this.parts.push({
    type: 'buffer',
    name: new Buffer(name, 'utf8'),
    fileName: typeof fileName === 'string' ? new Buffer(fileName, 'utf8') : null,
    buffer: buffer,
    size: buffer.length,
    mime: new Buffer(mime || 'application/octet-stream', 'ascii')
  });
};

Multipart.prototype.addStream = function addStream(name, size, stream, mime, fileName) {
  stream.pause();
  this.parts.push({
    type: 'stream',
    name: new Buffer(name, 'utf8'),
    fileName: typeof fileName === 'string' ? new Buffer(fileName, 'utf8') : null,
    stream: stream,
    size: size,
    mime: new Buffer(mime || 'application/octet-stream', 'ascii')
  });
};

Multipart.prototype.addText = function addText(name, text) {
  var buffer = new Buffer(text, 'ascii');
  this.addBuffer(name, buffer, 'text/plain; charset=UTF-8');
};

Multipart.prototype.contentTypeValuePrefix = new Buffer('multipart/form-data; boundary=', 'ascii');

Multipart.prototype.getContentType = function getContentType() {
  var buffer = new Buffer(this.contentTypeValuePrefix.length + this.boundary.length);
  this.contentTypeValuePrefix.copy(buffer);
  this.boundary.copy(buffer, this.contentTypeValuePrefix.length);
  return buffer;
};

Multipart.prototype.contentDispositionPrefix = new Buffer('Content-Disposition: form-data; name="', 'ascii');
Multipart.prototype.contentDispositionSuffix = new Buffer('"', 'ascii');
Multipart.prototype.contentDispositionFilenamePrefix = new Buffer('; filename="', 'ascii');
Multipart.prototype.contentDispositionFilenameSuffix = new Buffer('"', 'ascii');
Multipart.prototype.partContentTypePrefix = new Buffer('Content-Type: ', 'ascii');

Multipart.prototype.getContentLength = function getContentLength() {

  var self = this;
  var length = this.parts.reduce(function(sum, part) {
    var partLength = self.dash.length +
                     self.boundary.length +
                     self.crlf.length;

    partLength += self.contentDispositionPrefix.length +
                  part.name.length +
                  self.contentDispositionSuffix.length;
    if (part.fileName !== null) {
      partLength += self.contentDispositionFilenamePrefix.length +
                    part.fileName.length +
                    self.contentDispositionFilenameSuffix.length;
    }
    partLength += self.crlf.length;

    partLength += self.partContentTypePrefix.length +
                  part.mime.length +
                  self.crlf.length +
                  self.crlf.length;

    partLength += part.size + self.crlf.length;

    return sum + partLength;
  }, 0);

  length += self.dash.length +
            self.boundary.length +
            self.dash.length +
            self.crlf.length;

  return length;
};

Multipart.prototype.writeToStream = function writeToStream(stream, callback) {
  var self = this;
  var parts = this.parts;

  var entities = [];

  for (var i = 0; i < parts.length; i++) {
    var part = parts[i];

    entities.push(self.dash);
    entities.push(self.boundary);
    entities.push(self.crlf);

    entities.push(self.contentDispositionPrefix);
    entities.push(part.name);
    entities.push(self.contentDispositionSuffix);
    if (part.fileName !== null) {
      entities.push(self.contentDispositionFilenamePrefix);
      entities.push(part.fileName);
      entities.push(self.contentDispositionFilenameSuffix);
    }
    entities.push(self.crlf);

    entities.push(self.partContentTypePrefix);
    entities.push(part.mime);
    entities.push(self.crlf);
    entities.push(self.crlf);

    if (part.type === 'buffer') {
      entities.push(part.buffer);
      entities.push(self.crlf);
    }
    else {
      entities.push(part.stream);
      entities.push(self.crlf);
    }
  }

  entities.push(self.dash);
  entities.push(self.boundary);
  entities.push(self.dash);
  entities.push(self.crlf);

  function write(stream, entities) {
    var entity = entities[0];
    if (entity === undefined) {
      callback(null);
      return;
    }
    try {
      if (entity instanceof Buffer) {
        var buffer = entity;
        if (stream.write(buffer)) {
          write(stream, entities.slice(1));
        }
        else {
          stream.once('drain', function() {
            try {
              write(stream, entities.slice(1));
            }
            catch (err) {
              callback(err);
            }
          });
        }
      }
      else {
        var readableStream = entity;
        var readableStreamOnError = function(err) {
          try { readableStream.removeListener('error', readableStreamOnError) } catch (e) { }
          try { readableStream.removeListener('end', readableStreamOnEnd) } catch (e) { }
          try { readableStream.destroy() } catch (e) { }
          callback(err);
        };
        var readableStreamOnEnd = function() {
          try {
            readableStream.removeListener('error', readableStreamOnError);
            readableStream.removeListener('end', readableStreamOnEnd);
            write(stream, entities.slice(1));
          }
          catch (err) {
            callback(err);
          }
        };
        readableStream.on('error', readableStreamOnError);
        readableStream.on('end', readableStreamOnEnd);
        readableStream.pipe(stream, { end: false });
        readableStream.resume();
      }
    }
    catch (err) {
      callback(err);
    }
  }

  write(stream, entities);
};

Multipart.prototype.writeToStream = cb.wrap(Multipart.prototype.writeToStream);

module.exports = Multipart;

