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
      './bower_components/epoch/epoch.min.css',
      './bower_components/ng-sortable/dist/ng-sortable.min.css'
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
      // './bower_components/angular-resource/angular-resource.js',

      './bower_components/angular-ui-router/release/angular-ui-router.js',

      './bower_components/angular-strap/dist/modules/dimensions.js',
      './bower_components/angular-strap/dist/modules/button.js',
      './bower_components/angular-strap/dist/modules/tab.js',
      './bower_components/angular-strap/dist/modules/tab.tpl.js',
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
      './bower_components/angular-breadcrumb/release/angular-breadcrumb.js',
      './bower_components/angular-recursion/angular-recursion.js',

      './bower_components/ngstorage/ngStorage.js',
      './bower_components/angular-loading-bar/build/loading-bar.js',

      './bower_components/sockjs-client/dist/sockjs.js',

      './bower_components/ng-sortable/dist/ng-sortable.min.js',

      './bower_components/d3/d3.min.js',
      './bower_components/epoch/epoch.min.js'


    ].concat([
      './bower_components/cask-angular-*/*/module.js'
    ], mainBowerFiles({
        filter: /cask\-angular\-[^\/]+\/.*\.js$/
    })))
    .pipe(plug.concat('lib.js'))
    .pipe(gulp.dest('./dist/assets/bundle'));
});



/*
  application javascript
 */
gulp.task('js:app', function() {
  var PKG = JSON.stringify({
    name: pkg.name,
    v: pkg.version
  });
  return gulp.src([
      './app/main.js',
      './app/features/*/module.js',
      './app/**/*.js',
      '!./app/**/*-test.js'
    ])
    .pipe(plug.ngAnnotate())
    .pipe(plug.wrapper({
       header: '\n(function (PKG){ /* ${filename} */\n',
       footer: '\n})('+PKG+');\n'
    }))
    .pipe(plug.concat('app.js'))
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
      .pipe(plug.angularTemplatecache({
        module: pkg.name + '.commons'
      })),

    gulp.src([
      './app/features/home/home.html'
    ])
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
  Markup
 */
gulp.task('html:partials', function() {
  return gulp.src('./app/features/**/*.html')
      .pipe(gulp.dest('./dist/assets/features'));
});

gulp.task('html:main', function() {
  return gulp.src('./app/*.html')
      .pipe(gulp.dest('./dist'));
});

gulp.task('html', ['html:main', 'html:partials']);





/*
  JS hint
 */
gulp.task('lint', function() {
  return gulp.src(['./app/**/*.js', './server/*.js'])
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
gulp.task('lib', ['js:lib', 'css:lib']);
gulp.task('app', ['js:app', 'css:app']);
gulp.task('js', ['js:lib', 'js:app']);
gulp.task('css', ['css:lib', 'css:app']);
gulp.task('style', ['css']);

gulp.task('build', ['js', 'css', 'img', 'tpl', 'html']);
gulp.task('distribute', ['build', 'rev:replace']);

gulp.task('default', ['lint', 'build']);



/*
  watch
 */
gulp.task('watch', ['build'], function() {
  plug.livereload.listen();

  gulp.watch('./dist/**/*')
    .on('change', plug.livereload.changed);

  gulp.watch(['./app/**/*.js', '!./app/**/*-test.js'], ['js:app']);
  gulp.watch('./app/**/*.{less,css}', ['css:app']);
  gulp.watch(['./app/directives/**/*.html', './app/features/home/home.html'], ['tpl']);
  gulp.watch('./app/features/**/*.html', ['html:partials']);
  gulp.watch('./app/img/**/*', ['img']);

});
