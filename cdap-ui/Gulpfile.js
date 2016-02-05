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
    merge = require('merge-stream');

/*
  library CSS
 */
gulp.task('css:lib', ['fonts'], function() {

  return gulp.src([
      './app/styles/bootstrap.less',
      './bower_components/angular/angular-csp.css',
      './bower_components/angular-loading-bar/build/loading-bar.min.css',
      './bower_components/angular-motion/dist/angular-motion.min.css',
      './bower_components/font-awesome/css/font-awesome.min.css',
      './bower_components/ng-sortable/dist/ng-sortable.min.css',
      './bower_components/c3/c3.min.css',
      './bower_components/angular-gridster/dist/angular-gridster.min.css',
      './bower_components/angular-cron-jobs/dist/angular-cron-jobs.min.css',
    ].concat(mainBowerFiles({
      filter: /cask\-angular\-[^\/]+\/.*\.(css|less)$/
    })))
    .pipe(plug.if('*.less', plug.less()))
    .pipe(plug.concat('lib.css'))
    .pipe(gulp.dest('./dist/assets/bundle'));
});


/*
  fonts
 */
gulp.task('fonts', function() {
  return gulp.src([
      // './bower_components/bootstrap/dist/fonts/*',
      './app/styles/fonts/*',
      './bower_components/font-awesome/fonts/*'
    ])
    .pipe(gulp.dest('./dist/assets/fonts'));
});


/*
  application CSS
 */
gulp.task('css:app', function() {
  return gulp.src([
      './app/styles/common.less',
      './app/styles/themes/*.less',
      './app/directives/**/*.{less,css}',
      './app/features/**/*.{less,css}'
    ])
    .pipe(plug.if('*.less', plug.less()))
    .pipe(plug.concat('app.css'))
    .pipe(plug.autoprefixer(["> 1%"], {cascade:true}))
    .pipe(gulp.dest('./dist/assets/bundle'));
});



/*
  library javascript
 */
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

      './bower_components/ng-sortable/dist/ng-sortable.min.js',

      './bower_components/d3/d3.min.js',

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
      './bower_components/ace-builds/src-min-noconflict/ace.js',
      './bower_components/angular-ui-ace/ui-ace.js',
      './bower_components/jsPlumb/dist/js/jsPlumb-2.0.4-min.js',
      './bower_components/angular-gridster/dist/angular-gridster.min.js',
      './bower_components/angular-cron-jobs/dist/angular-cron-jobs.min.js',
      './bower_components/angularjs-dropdown-multiselect/dist/angularjs-dropdown-multiselect.min.js',
      './bower_components/marked/marked.min.js',
      './bower_components/angular-marked/dist/angular-marked.min.js'

    ].concat([
      './bower_components/cask-angular-*/*/module.js'
    ], mainBowerFiles({
        filter: /cask\-angular\-[^\/]+\/.*\.js$/
    })))
    .pipe(plug.concat('lib.js'))
    .pipe(gulp.dest('./dist/assets/bundle'));
});

gulp.task('js:aceworkers', function() {
  gulp.src([
    './bower_components/ace-builds/src-min-noconflict/ace.js',
    './bower_components/ace-builds/src-min-noconflict/mode-javascript.js',
    './bower_components/ace-builds/src-min-noconflict/worker-javascript.js'
  ])
    .pipe(gulp.dest('./dist/assets/bundle/ace-editor-worker-scripts/'));
});

gulp.task('js:$modal', function() {
  gulp.src([
    './bower_components/angular-bootstrap/ui-bootstrap-tpls.js',
  ])
  .pipe(plug.replace('$tooltip', '$bootstrapTooltip'))
  .pipe(plug.ngAnnotate({rename: [{from: '$modal', to: '$bootstrapModal'}]}))
  .pipe(plug.concat('ui-bootstrap-tpls.js'))
  .pipe(gulp.dest('./bower_components/angular-bootstrap'));
});

function getEs6Features(isNegate) {
  /*
    When we move a feature to use es6 all we need to do is add it here for the build/dev process.
    Eventually when all the features are ported to es6 then we should use a better package manager
    like webpack or jspm for development and replace the js:app,js:lib,js:aceworkers & js:$modal gulp tasks
    with the package manager build step. The transition process is going to be painful but the end goal is better (hopefully :])
  */
  var es6features = [
    'workflows',
    'flows',
    'apps',
    'search',
    'pins',
    'hydrator'
  ];
  var returnVal = [];
  es6features.forEach(function(feature) {
    returnVal = returnVal.concat([
      (isNegate? '!': '') + './app/features/' + feature + '/module.js',
      (isNegate? '!': '') + './app/features/' + feature + '/**/*js',
    ]);
  });
  return returnVal;
}

function getEs6Directives(isNegate) {
  var es6directives = [
    (isNegate ? '!' : '') + './app/directives/dag/**/*.js'
  ];

  return es6directives;
}


/*
  application javascript
 */
gulp.task('watch:js:app', function() {
  var PKG = JSON.stringify({
    name: pkg.name,
    v: pkg.version
  });

  var source = [
    './app/main.js',
    '!./app/lib/c3.js',
    './app/features/*/module.js',
    './app/**/*.js',
    '!./app/**/*-test.js'
  ];
  source = source.concat(getEs6Features(true));
  source = source.concat(getEs6Directives(true));

  return gulp.src(source)
    .pipe(plug.plumber())
    .pipe(plug.ngAnnotate())
    .pipe(plug.wrapper({
       header: '\n(function (PKG){ /* ${filename} */\n',
       footer: '\n})('+PKG+');\n'
    }))
    .pipe(plug.concat('app.js'))
    .pipe(gulp.dest('./dist/assets/bundle'));
});

gulp.task('watch:js:app:babel', function() {
  var PKG = JSON.stringify({
    name: pkg.name,
    v: pkg.version
  });

  var source = getEs6Features();
  source = source.concat(getEs6Directives());

  return gulp.src(source)
    .pipe(plug.plumber())
    .pipe(plug.ngAnnotate())
    // .pipe(plug.sourcemaps.init())
    .pipe(plug.wrapper({
       header: '\n(function (PKG){ /* ${filename} */\n',
       footer: '\n})('+PKG+');\n'
    }))
    .pipe(plug.babel())
    .pipe(plug.concat('app.es6.js'))
    // .pipe(plug.sourcemaps.write("."))
    .pipe(gulp.dest('./dist/assets/bundle'));
});

gulp.task('js:app', function() {
  var PKG = JSON.stringify({
    name: pkg.name,
    v: pkg.version
  });
  return gulp.src([
    './app/main.js',
    '!./app/lib/c3.js',
    './app/features/*/module.js',
    './app/**/*.js',
    '!./app/**/*-test.js'
  ])
    .pipe(plug.plumber())
    .pipe(plug.ngAnnotate())
    // .pipe(plug.sourcemaps.init())
    .pipe(plug.wrapper({
       header: '\n(function (PKG){ /* ${filename} */\n',
       footer: '\n})('+PKG+');\n'
    }))
    .pipe(plug.babel())
    .pipe(plug.concat('app.js'))
    // .pipe(plug.sourcemaps.write("."))
    .pipe(gulp.dest('./dist/assets/bundle'));
});


/*
  images
  TODO: imgmin?
 */
gulp.task('img', function() {
  return gulp.src('./app/styles/img/**/*')
    .pipe(gulp.dest('./dist/assets/img'));
});




/*
  template cache
 */
gulp.task('tpl', function() {
  return merge(

    gulp.src([
      './app/directives/**/*.html'
    ])
      .pipe(plug.minifyHtml({loose: true, quotes: true}))
      .pipe(plug.angularTemplatecache({
        module: pkg.name + '.commons'
      })),

    gulp.src([
      './app/features/home/home.html'
    ])
      .pipe(plug.minifyHtml({loose: true, quotes: true}))
      .pipe(plug.angularTemplatecache({
        module: pkg.name + '.features',
        base: __dirname + '/app',
        root: '/assets/'
      }))

  )
    .pipe(plug.concat('tpl.js'))
    .pipe(gulp.dest('./dist/assets/bundle'));
});


/*
  Polyfill
*/
gulp.task('polyfill', function () {
  return gulp.src('./app/polyfill.js')
      .pipe(gulp.dest('./dist/assets/bundle'));
});



/*
  Markup
 */
gulp.task('html:partials', function() {
  return gulp.src('./app/features/**/*.html')
      .pipe(plug.minifyHtml({loose: true, quotes: true}))
      .pipe(gulp.dest('./dist/assets/features'));
});

gulp.task('html:main', function() {
  return gulp.src('./app/*.html')
      .pipe(plug.minifyHtml({loose: true, quotes: true}))
      .pipe(gulp.dest('./dist'));
});

gulp.task('html:main.dev', function () {
  return gulp.src('./app/*.html')
      .pipe(plug.replace('<!-- DEV DEPENDENCIES -->',
        '<script type="text/javascript" src="/assets/bundle/app.es6.js"></script>' +
        '<script src="http://localhost:35729/livereload.js"></script>'))
      .pipe(gulp.dest('./dist'));
    });

gulp.task('html', ['html:main', 'html:partials']);
gulp.task('html.dev', ['html:main.dev', 'html:partials'])





/*
  JS hint
 */
gulp.task('lint', function() {
  return gulp.src(['./app/**/*.js', './server/*.js'])
    .pipe(plug.plumber())
    .pipe(plug.jshint())
    .pipe(plug.jshint.reporter())
    .pipe(plug.jshint.reporter('fail'));
});
gulp.task('jshint', ['lint']);
gulp.task('hint', ['lint']);




/*
  clean dist
 */
gulp.task('clean', function(cb) {
  del(['./dist/*'], cb);
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
    .pipe(plug.minifyCss({keepBreaks:true}))
    .pipe(gulp.dest('./dist/assets/bundle'));
});

gulp.task('minify', ['js:minify', 'css:minify']);




/*
  rev'd assets
 */

gulp.task('rev:manifest', ['minify', 'tpl'], function() {
  return gulp.src(['./dist/assets/bundle/*'])
    .pipe(plug.rev())
    .pipe(plug.size({showFiles:true, gzip:true, total:true}))
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
  };
  return out.pipe(gulp.dest('./dist'));
});



/*
  alias tasks
 */
gulp.task('js', ['js:$modal', 'js:lib', 'js:aceworkers', 'js:app', 'polyfill']);
gulp.task('watch:js', ['js:$modal', 'js:lib', 'js:aceworkers', 'watch:js:app', 'watch:js:app:babel', 'polyfill']);
gulp.task('css', ['css:lib', 'css:app']);
gulp.task('style', ['css']);


gulp.task('watch:build', ['watch:js', 'css', 'img', 'tpl', 'html.dev']);
gulp.task('build', ['js', 'css', 'img', 'tpl', 'html']);

gulp.task('distribute', ['clean', 'build', 'rev:replace']);

gulp.task('default', ['lint', 'build']);



/*
  watch
 */
gulp.task('watch', ['jshint', 'watch:build'], function() {
  plug.livereload.listen();

  gulp.watch('./dist/**/*')
    .on('change', plug.livereload.changed);

  gulp.watch([
    './app/**/*.js',
    '!./app/features/workflows/**/*.js',
    '!./app/features/hydrator/**/*.js',
    '!./app/features/apps/**/*.js',
    '!./app/features/search/**/*.js',
    '!./app/features/pins/**/*.js',
    '!./app/features/flows/**/*.js',
    '!./app/directives/dag/**/*.js',
    '!./app/**/*-test.js'
  ], ['jshint', 'watch:js:app']);
  gulp.watch([
    './app/features/workflows/**/*.js',
    './app/features/hydrator/**/*.js',
    './app/features/apps/**/*.js',
    './app/features/pins/**/*.js',
    './app/features/search/**/*.js',
    './app/features/flows/**/*.js',
    './app/features/flows/**/*.js',
    './app/directives/dag/**/*.js'
  ], ['jshint', 'watch:js:app:babel']);

  gulp.watch('./app/**/*.{less,css}', ['css']);
  gulp.watch(['./app/directives/**/*.html', './app/features/home/home.html'], ['tpl']);
  gulp.watch('./app/features/**/*.html', ['html:partials']);
  gulp.watch('./app/img/**/*', ['img']);
  gulp.watch('./app/index.html', ['html:main.dev']);

});
