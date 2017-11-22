/*
 * Copyright © 2015 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

var gulp = require('gulp'),
    plug = require('gulp-load-plugins')(),
    pkg = require('./package.json'),
    del = require('del'),
    mainBowerFiles = require('main-bower-files'),
    merge = require('merge-stream'),
    autoprefixer = require('autoprefixer');

function getEs6Directives(isNegate) {
  var es6directives = [
    'dag-plus',
    'plugin-templates',
    'my-global-navbar',
    'datetime-picker',
    'datetime-range',
    'log-viewer',
    'complex-schema',
    'my-pipeline-settings',
    'my-pipeline-summary',
    'my-pipeline-resource',
    'my-post-run-action-wizard',
    'my-post-run-actions',
    'widget-container/widget-complex-schema-editor',
    'timeline',
    'widget-container',
    'plugin-functions',
    'my-link-button'
  ];

  return es6directives.map(function (directive) {
    return (isNegate ? '!' : '') + './app/directives/' + directive + '/**/*.js';
  });
}
function getExtensionBuildPipeline(extension) {
  var PKG = JSON.stringify({
    name: pkg.name,
    v: pkg.version
  });

  var source = [
    './app/'+ extension + '/main.js',
    './app/services/**/*.js',
    './app/directives/**/*.js',
    './app/filters/**/*.js',
    './app/lib/global-lib.js',
    './app/' + extension + '/module.js',
    './app/' + extension + '/**/*.js',
  ];
  source = source.concat(getEs6Directives(true));

  return gulp.src(source)
    .pipe(plug.plumber())
    .pipe(plug.wrapper({
       header: '\n(function (PKG){ /* ${filename} */\n',
       footer: '\n})('+PKG+');\n'
    }))
    .pipe(plug.babel())
    .pipe(plug.ngAnnotate())
    .pipe(plug.concat(extension + '.js'))
    .pipe(gulp.dest('./dist/assets/bundle'));
}
function getBabelBuildPipeline() {
  var PKG = JSON.stringify({
    name: pkg.name,
    v: pkg.version
  });

  var source = getEs6Directives();
  source = source.concat(
    [
      './app/features/userprofile/module.js',
      './app/features/userprofile/**/*.js'
    ]
  );
  return gulp.src(source)
    .pipe(plug.plumber())
    .pipe(plug.wrapper({
       header: '\n(function (PKG){ /* ${filename} */\n',
       footer: '\n})('+PKG+');\n'
    }))
    .pipe(plug.babel())
    .pipe(plug.ngAnnotate())
    .pipe(plug.concat('common.es6.js'))
    .pipe(gulp.dest('./dist/assets/bundle'));
}

gulp.task('css:lib', ['fonts'], function() {

  return merge(
    gulp.src([
      './bower_components/angular/angular-csp.css',
      './bower_components/angular-loading-bar/build/loading-bar.min.css',
      './bower_components/angular-motion/dist/angular-motion.min.css',
      './node_modules/font-awesome/css/font-awesome.min.css',
      './bower_components/c3/c3.min.css',
      './bower_components/angular-gridster/dist/angular-gridster.min.css',
      './bower_components/angular-cron-jobs/dist/angular-cron-jobs.min.css',
    ].concat(mainBowerFiles({
      filter: /cask\-angular\-[^\/]+\/.*\.(css|less)$/
    }))),
    gulp.src('./app/styles/bootstrap.less')
      .pipe(plug.less())
  )
    .pipe(plug.concat('lib.css'))
    .pipe(gulp.dest('./dist/assets/bundle'));
});
gulp.task('css:app', ['css:lint'], function() {
  var processor = [
    autoprefixer({ browsers: ['> 1%'], cascade:true })
  ];

  return gulp.src([
      './app/styles/common.less',
      './app/styles/themes/*.less',
      './app/directives/**/*.less',
      './app/hydrator/**/*.less',
      './app/tracker/**/*.less',
      './app/logviewer/**/*.less'
    ])
    .pipe(plug.less())
    .pipe(plug.concat('app.css'))
    .pipe(plug.postcss(processor))
    .pipe(gulp.dest('./dist/assets/bundle'))
    .pipe(plug.livereload({ port: 35728 }));
});
gulp.task('css:lint', function () {

  return gulp.src([
      './app/styles/common.less',
      './app/styles/themes/*.less',
      './app/directives/**/*.{less,css}',
      './app/features/**/*.{less,css}'
    ])
    .pipe(plug.stylelint({
      configFile: './.stylelintrc',
      syntax: 'less',
      reporters: [{
        formatter: 'string',
        console: true
      }]
    }));
});
gulp.task('js:lib', function() {
  return gulp.src([
      './bower_components/angular/angular.js',

      './bower_components/angular-sanitize/angular-sanitize.js',
      './bower_components/angular-animate/angular-animate.js',
      './bower_components/angular-resource/angular-resource.js',

      './bower_components/angular-ui-router/release/angular-ui-router.js',

      './bower_components/angular-strap/dist/modules/compiler.js',
      './bower_components/angular-strap/dist/modules/dimensions.js',
      './bower_components/angular-strap/dist/modules/tooltip.js',
      './bower_components/angular-strap/dist/modules/tooltip.tpl.js',
      './bower_components/angular-strap/dist/modules/dropdown.js',
      './bower_components/angular-strap/dist/modules/dropdown.tpl.js',
      './bower_components/angular-strap/dist/modules/modal.js',
      './bower_components/angular-strap/dist/modules/modal.tpl.js',
      './bower_components/angular-strap/dist/modules/alert.js',
      './bower_components/angular-strap/dist/modules/alert.tpl.js',
      './bower_components/angular-strap/dist/modules/popover.js',
      './bower_components/angular-strap/dist/modules/popover.tpl.js',
      './bower_components/angular-strap/dist/modules/collapse.js',
      './bower_components/angular-strap/dist/modules/parse-options.js',
      './bower_components/angular-strap/dist/modules/typeahead.js',
      './bower_components/angular-strap/dist/modules/typeahead.tpl.js',
      './bower_components/angular-strap/dist/modules/select.js',
      './bower_components/angular-strap/dist/modules/select.tpl.js',


      './bower_components/angular-strap/dist/modules/date-parser.js',
      './bower_components/angular-strap/dist/modules/date-formatter.js',
      './bower_components/angular-strap/dist/modules/datepicker.js',
      './bower_components/angular-strap/dist/modules/datepicker.tpl.js',
      './bower_components/angular-strap/dist/modules/timepicker.js',
      './bower_components/angular-strap/dist/modules/timepicker.tpl.js',

      './bower_components/angular-breadcrumb/release/angular-breadcrumb.js',

      './bower_components/ngstorage/ngStorage.js',
      './bower_components/angular-loading-bar/build/loading-bar.js',

      './bower_components/sockjs-client/dist/sockjs.js',

      './bower_components/d3/d3.min.js',
      // './bower_components/d3-tip/index.js', FIXME: add this when we fix multiple versions of d3 issue.
      './bower_components/d3-timeline/src/d3-timeline.js',

      './bower_components/epoch/epoch.min.js',
      './bower_components/lodash/lodash.js',
      './bower_components/graphlib/dist/graphlib.core.js',
      './bower_components/dagre/dist/dagre.core.js',
      './bower_components/dagre-d3/dist/dagre-d3.core.js',
      './bower_components/moment/moment.js',
      './bower_components/angular-moment/angular-moment.js',
      './bower_components/angular-bootstrap/ui-bootstrap-tpls.js',

      './bower_components/node-uuid/uuid.js',
      './bower_components/angular-cookies/angular-cookies.min.js',
      // './bower_components/c3/c3.js',
      './app/lib/c3.js',
      './node_modules/redux/dist/redux.min.js',
      './node_modules/redux-thunk/dist/redux-thunk.min.js',
      './bower_components/ace-builds/src-min-noconflict/ace.js',
      './bower_components/angular-ui-ace/ui-ace.js',
      './bower_components/jsPlumb/dist/js/jsPlumb-2.0.6-min.js',
      './bower_components/angular-gridster/dist/angular-gridster.min.js',
      './bower_components/angular-cron-jobs/dist/angular-cron-jobs.min.js',
      './bower_components/angularjs-dropdown-multiselect/dist/angularjs-dropdown-multiselect.min.js',
      './bower_components/marked/marked.min.js',
      './bower_components/angular-marked/dist/angular-marked.min.js',

      './bower_components/js-beautify/js/lib/beautify.js',
      './bower_components/angular-file-saver/dist/angular-file-saver.bundle.js',
      './bower_components/ngInfiniteScroll/build/ng-infinite-scroll.min.js',
      './bower_components/angular-inview/angular-inview.js',
      './bower_components/esprima/esprima.js',
      './node_modules/react/dist/react-with-addons.min.js',
      './node_modules/react-dom/dist/react-dom.min.js',
      './node_modules/ngreact/ngReact.min.js',

      './node_modules/cdap-avsc/dist/cdap-avsc-lib.js',
      './node_modules/svg4everybody/dist/svg4everybody.min.js'
    ].concat([
      './bower_components/cask-angular-*/*/module.js'
    ], mainBowerFiles({
        filter: /cask\-angular\-[^\/]+\/.*\.js$/
    })))
    .pipe(plug.replace('glyphicon', 'fa'))
    .pipe(plug.concat('lib.js'))
    .pipe(gulp.dest('./dist/assets/bundle'));
});
gulp.task('js:aceworkers', function() {
  gulp.src([
    './bower_components/ace-builds/src-min-noconflict/ace.js',
    './bower_components/ace-builds/src-min-noconflict/mode-javascript.js',
    './bower_components/ace-builds/src-min-noconflict/worker-javascript.js',
    './bower_components/ace-builds/src-min-noconflict/mode-python.js',
    './bower_components/ace-builds/src-min-noconflict/mode-scala.js'
  ])
    .pipe(gulp.dest('./dist/assets/bundle/ace-editor-worker-scripts/'));
});
gulp.task('fonts', function() {
  return gulp.src([
      // './bower_components/bootstrap/dist/fonts/*',
      './app/styles/fonts/*',
      './node_modules/font-awesome/fonts/*'
    ])
    .pipe(gulp.dest('./dist/assets/fonts'));
});

// Build using watch
gulp.task('watch:js:app:hydrator', function() {
  return getExtensionBuildPipeline('hydrator')
    .pipe(plug.livereload({ port: 35728 }));
});
gulp.task('watch:js:app:tracker', function() {
  return getExtensionBuildPipeline('tracker')
    .pipe(plug.livereload({ port: 35728 }));
});
gulp.task('watch:js:app:logviewer', function() {
  return getExtensionBuildPipeline('logviewer')
    .pipe(plug.livereload({ port: 35728 }));
});
gulp.task('watch:js:app:babel', function() {
  return getBabelBuildPipeline()
    .pipe(plug.livereload({ port: 35728 }));
});

// Build once.
gulp.task('js:app:hydrator', function() {
  return getExtensionBuildPipeline('hydrator');
});
gulp.task('js:app:tracker', function() {
  return getExtensionBuildPipeline('tracker');
});
gulp.task('js:app:logviewer', function() {
  return getExtensionBuildPipeline('logviewer');
});
gulp.task('js:app:babel', function() {
  return getBabelBuildPipeline();
});

gulp.task('js:app', ['js:app:babel', 'js:app:hydrator', 'js:app:tracker', 'js:app:logviewer']);
gulp.task('watch:js:app', [
  'watch:js:app:hydrator',
  'watch:js:app:tracker',
  'watch:js:app:logviewer'
]);
gulp.task('polyfill', function () {
  return gulp.src([
    './app/polyfill.js',
    './app/ui-utils/url-generator.js'
  ])
    .pipe(plug.babel())
    .pipe(plug.concat('polyfill.js'))
    .pipe(gulp.dest('./dist/assets/bundle'));
});

gulp.task('img', function() {
  return gulp.src('./app/styles/img/**/*')
    .pipe(gulp.dest('./dist/assets/img'));
});

gulp.task('html:partials', function() {
  return gulp.src([
    './app/{hydrator,tracker,logviewer}/**/*.html',
    './app/features/{userprofile,}/**/*.html',
    '!./app/tracker/tracker.html',
    '!./app/hydrator/hydrator.html',
    '!./app/logviewer/logviewer.html'
    ])
      .pipe(plug.htmlmin({ removeComments: true }))
      .pipe(gulp.dest('./dist/assets/features'))
      .pipe(plug.livereload({ port: 35728 }));
});
gulp.task('html:main', function() {
  return gulp.src([
    './app/tracker/tracker.html',
    './app/hydrator/hydrator.html',
    './app/logviewer/logviewer.html',
  ])
    .pipe(plug.htmlmin({ removeComments: true }))
    .pipe(gulp.dest('./dist'));
});
gulp.task('html', ['html:main', 'html:partials']);
gulp.task('tpl', function() {
  return gulp.src([
      './app/directives/**/*.html',
      './app/services/**/*.html'
    ])
    .pipe(plug.htmlmin({ removeComments: true }))
    .pipe(plug.angularTemplatecache({
      module: pkg.name + '.commons'
    }))
    .pipe(plug.concat('tpl.js'))
    .pipe(gulp.dest('./dist/assets/bundle'))
    .pipe(plug.livereload({ port: 35728 }));
});

gulp.task('js', ['js:lib', 'js:aceworkers', 'js:app', 'polyfill']);
gulp.task('watch:js', ['watch:js:app', 'watch:js:app:babel', 'polyfill']);

gulp.task('css', ['css:lib', 'css:app']);
gulp.task('style', ['css']);
gulp.task('build', ['js', 'css', 'img', 'tpl', 'html']);

gulp.task('lint', function() {
  return gulp.src([
    './app/**/*.js',
    '!./app/cdap/**/*.js',
    '!./app/login/**/*.js',
    '!./app/lib/**/*.js',
    '!./app/common/**/*.js',
    '!./app/wrangler/**/*.js',
    './server/*.js'
  ])
    .pipe(plug.plumber())
    .pipe(plug.jshint())
    .pipe(plug.jshint.reporter())
    .pipe(plug.jshint.reporter('fail'));
});
gulp.task('jshint', ['lint']);
gulp.task('hint', ['lint']);

gulp.task('clean', function() {
  return del.sync(['./dist/*']);
});

/*
  minification
 */
gulp.task('js:minify', ['js'], function() {
  return gulp.src('./dist/assets/bundle/{app,lib}.js')
    .pipe(plug.uglify())
    .pipe(gulp.dest('./dist/assets/bundle'));
});
gulp.task('css:minify', ['css'], function() {
  return gulp.src('./dist/assets/bundle/*.css')
    .pipe(plug.cssnano({ safe: true }))
    .pipe(gulp.dest('./dist/assets/bundle'));
});
gulp.task('fonts:minify', ['fonts'], function() {
  return gulp.src('./dist/assets/fonts/*.svg')
    .pipe(plug.svgmin({
      plugins: [{
        removeUselessDefs: false
      }, {
        cleanupIDs: false
      }]
    }))
    .pipe(gulp.dest('./dist/assets/fonts'));
});
gulp.task('img:minify', ['img'], function() {
  return gulp.src('./dist/assets/img/*.svg')
    .pipe(plug.svgmin({
      plugins: [{
        removeUselessDefs: false
      }, {
        cleanupIDs: false
      }]
    }))
    .pipe(gulp.dest('./dist/assets/img'));
});
gulp.task('minify', ['js:minify', 'css:minify', 'fonts:minify', 'img:minify']);

/*
  rev'd assets
 */
gulp.task('rev:manifest', ['minify', 'tpl'], function() {
  return gulp.src(['./dist/assets/bundle/*'])
    .pipe(plug.rev())
    .pipe(gulp.dest('./dist/assets/bundle'))  // write rev'd assets to build dir

    .pipe(plug.rev.manifest({path:'manifest.json'}))
    .pipe(gulp.dest('./dist/assets/bundle')); // write manifest

});
gulp.task('rev:replace', ['html:main', 'rev:manifest'], function() {
  var rev = require('./dist/assets/bundle/manifest.json'),
      out = gulp.src('./dist/*.html'),
      p = '/assets/bundle/';
  for (var f in rev) {
    out = out.pipe(plug.replace(p+f, p+rev[f]));
  }
  return out.pipe(gulp.dest('./dist'));
});
gulp.task('watch:build', ['watch:js', 'css', 'img', 'tpl', 'html']);
gulp.task('distribute', ['clean', 'build', 'rev:replace']);
gulp.task('default', ['lint', 'build']);
/*
  watch
 */
gulp.task('watch', ['jshint', 'build'], function() {
  plug.livereload.listen({ port: 35728 });

  var jsAppSource = [
    './app/**/*.js',
    '!./app/cdap/**/*.js',
    '!./app/login/**/*.js',
    '!./app/wrangler/**/*.js',
    '!./app/**/*-test.js'
  ];
  jsAppSource = jsAppSource.concat(getEs6Directives(true));

  gulp.watch(jsAppSource, ['jshint', 'watch:js:app']);

  var jsAppBabelSource = [];
  jsAppBabelSource = jsAppBabelSource.concat(getEs6Directives(false));
  gulp.watch(jsAppBabelSource, ['jshint', 'watch:js:app:babel']);

  gulp.watch('./app/**/*.{less,css}', ['css']);
  gulp.watch([
    './app/directives/**/*.html',
    './app/services/**/*.html'
  ], ['tpl']);
  gulp.watch([
    './app/hydrator/**/*.html',
    './app/tracker/**/*.html',
    './app/logviewer/**/*.html'
  ], ['html:partials', 'html:main']);
});
