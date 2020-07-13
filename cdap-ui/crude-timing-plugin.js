let CrudeTimingPlugin = function () { };

CrudeTimingPlugin.prototype.apply = function (compiler) {
  compiler.plugin('compilation', (compilation) => {
    let startOptimizePhase;

    compilation.plugin('optimize-chunk-assets', (chunks, callback) => {
      // Cruddy way of measuring minification time. UglifyJSPlugin does all
      // its work in this phase of compilation so we time the duration of
      // the entire phase
      startOptimizePhase = Date.now();

      // For async phases: don't forget to invoke the callback
      callback();
    });

    compilation.plugin('after-optimize-chunk-assets', () => {
      const optimizePhaseDuration = Date.now() - startOptimizePhase;
      console.log(`optimize-chunk-asset phase duration: ${optimizePhaseDuration}`);
    });
  });
};

module.exports = CrudeTimingPlugin;
