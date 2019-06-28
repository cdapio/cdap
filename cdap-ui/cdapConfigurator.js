const parser = require('./server/config/parser.js');
const cdapConfig = parser.extractConfig('cdap');

module.exports = {
    cdapConfig
}
