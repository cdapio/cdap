const parser = require('./server/config/parser.js');
let cdapConfig;

async function getCDAPConfig() {
  if (cdapConfig) {
    return cdapConfig;
  }

  try {
    cdapConfig = await parser.extractConfig('cdap');
  } catch (e) {
    return Promise.reject(e);
  }

  return cdapConfig;
}

module.exports = {
    getCDAPConfig
}
