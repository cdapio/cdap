Module = require('module')

module.exports = {
  ohai: 'The flying spaghetti monster was here.'
};

try {
  module.exports.explicitSimpleDep = require('simple_dep');
} catch (err) {
  module.exports.explicitSimpleDep = err;
}

try {
  module.exports.autoSimpleDep = simpleDep;
} catch (err) {
  module.exports.autoSimpleDep = err;
}
