/*
 * Copyright © 2016 Cask Data, Inc.
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

var webpack = require('webpack');
var CopyWebpackPlugin = require('copy-webpack-plugin');
var path = require('path');
var LiveReloadPlugin = require('webpack-livereload-plugin');
var HtmlWebpackPlugin = require('html-webpack-plugin');
var StyleLintPlugin = require('stylelint-webpack-plugin');
var CaseSensitivePathsPlugin = require('case-sensitive-paths-webpack-plugin');
var uuidV4 = require('uuid/v4');
var TerserPlugin = require('terser-webpack-plugin');
const { CleanWebpackPlugin } = require('clean-webpack-plugin');
var ForkTsCheckerWebpackPlugin = require('fork-ts-checker-webpack-plugin');
var LodashModuleReplacementPlugin = require('lodash-webpack-plugin');
const WebpackBundleAnalyzer = require('webpack-bundle-analyzer');

// the clean options to use
let cleanOptions = {
  verbose: true,
  dry: false,
};

const loaderExclude = [
  /node_modules/,
  /bower_components/,
  /packaged\/public\/dist/,
  /packaged\/public\/cdap_dist/,
  /packaged\/public\/common_dist/,
  /lib/,
];

var mode = process.env.NODE_ENV || 'production';
const isModeProduction = (mode) => mode === 'production' || mode === 'non-optimized-production';
const getWebpackDllPlugins = (mode) => {
  var sharedDllManifestFileName = 'shared-vendor-manifest.json';
  var cdapDllManifestFileName = 'cdap-vendor-manifest.json';
  if (mode === 'development') {
    sharedDllManifestFileName = 'shared-vendor-development-manifest.json';
    cdapDllManifestFileName = 'cdap-vendor-development-manifest.json';
  }
  return [
    new webpack.DllReferencePlugin({
      context: path.resolve(__dirname, 'packaged', 'public', 'dll'),
      manifest: require(path.join(
        __dirname,
        'packaged',
        'public',
        'dll',
        sharedDllManifestFileName
      )),
    }),
    new webpack.DllReferencePlugin({
      context: path.resolve(__dirname, 'packaged', 'public', 'dll'),
      manifest: require(path.join(__dirname, 'packaged', 'public', 'dll', cdapDllManifestFileName)),
    }),
  ];
};
var plugins = [
  new CleanWebpackPlugin(cleanOptions),
  new CaseSensitivePathsPlugin(),
  ...getWebpackDllPlugins(mode),
  new LodashModuleReplacementPlugin({
    shorthands: true,
    collections: true,
    caching: true,
  }),
  new CopyWebpackPlugin(
    [
      {
        from: './styles/fonts',
        to: './fonts/',
      },
      {
        from: path.resolve(__dirname, 'node_modules', 'font-awesome', 'fonts'),
        to: './fonts/',
      },
      {
        from: './styles/img',
        to: './img/',
      },
      {
        from: './**/*-web-worker.js',
        to: './web-workers/',
        flatten: true,
      },
    ],
    {
      copyUnmodified: true,
    }
  ),
  new StyleLintPlugin({
    syntax: 'scss',
    files: ['**/*.scss'],
  }),
  new HtmlWebpackPlugin({
    title: 'CDAP',
    template: './cdap.html',
    filename: 'cdap.html',
    hash: true,
    inject: false,
    hashId: uuidV4(),
    mode: isModeProduction(mode) ? '' : 'development.',
  }),
];
if (!isModeProduction(mode)) {
  plugins.push(
    new ForkTsCheckerWebpackPlugin({
      tsconfig: __dirname + '/tsconfig.json',
      tslint: __dirname + '/tslint.json',
      tslintAutoFix: true,
      // watch: ["./app/cdap"], // optional but improves performance (less stat calls)
      memoryLimit: 4096,
    })
  );
}

var rules = [
  {
    test: /\.s?css$/,
    use: ['style-loader', 'css-loader', 'postcss-loader', 'sass-loader'],
  },
  {
    test: /\.ya?ml$/,
    use: 'yml-loader',
  },
  {
    enforce: 'pre',
    test: /\.js$/,
    loader: 'eslint-loader',
    options: {
      fix: true,
    },
    exclude: loaderExclude,
  },
  {
    test: /\.js$/,
    use: ['babel-loader'],
    exclude: loaderExclude,
    include: [path.join(__dirname, 'app'), path.join(__dirname, '.storybook')],
  },
  {
    test: /\.tsx?$/,
    use: [
      'babel-loader',
      {
        loader: 'ts-loader',
        options: {
          transpileOnly: true,
          experimentalWatchApi: true
        },
      },
    ],
    exclude: loaderExclude,
    include: [path.join(__dirname, 'app'), path.join(__dirname, '.storybook')],
  },
  {
    test: /\.woff(2)?(\?v=[0-9]\.[0-9]\.[0-9])?$/,
    use: [
      {
        loader: 'url-loader',
        options: {
          limit: 10000,
          mimetype: 'application/font-woff',
        },
      },
    ],
  },
  {
    test: /\.(ttf|eot)(\?v=[0-9]\.[0-9]\.[0-9])?$/,
    use: 'url-loader',
  },
  {
    test: /\.svg/,
    use: [
      {
        loader: 'svg-sprite-loader',
      },
    ],
  },
];

if (isModeProduction(mode)) {
  plugins.push(
    new webpack.DefinePlugin({
      'process.env': {
        NODE_ENV: JSON.stringify('production'),
        __DEVTOOLS__: false,
      },
    })
  );
}

if (mode === 'development') {
  plugins.push(
    new LiveReloadPlugin({
      port: 35728,
      appendScriptTag: true,
      delay: 500,
      ignore:
        '/node_modules/|/bower_components/|/packaged/public/dist/|/packaged/public/cdap_dist/|/packaged/public/common_dist/|/lib/',
    })
  );
  plugins.push(new WebpackBundleAnalyzer.BundleAnalyzerPlugin());
}

var webpackConfig = {
  mode: isModeProduction(mode) ? 'production' : 'development',
  devtool: 'eval-source-map',
  context: __dirname + '/app/cdap',
  entry: {
    cdap: ['@babel/polyfill', './cdap.js'],
  },
  module: {
    rules,
  },
  output: {
    filename: '[name].[chunkhash].js',
    chunkFilename: '[name].[chunkhash].js',
    path: __dirname + '/packaged/public/cdap_dist/cdap_assets/',
    publicPath: '/cdap_assets/',
  },
  stats: {
    assets: false,
    children: false,
    chunkGroups: false,
    chunkModules: false,
    chunkOrigins: false,
    chunks: false,
    modules: false,
  },
  plugins: plugins,
  // TODO: Need to investigate this more.
  optimization: {
    moduleIds: "hashed",
    runtimeChunk: {
      name: "manifest",
    },
    splitChunks: {// { // false
      cacheGroups: {
        vendor: {
          name: "node_vendors", // part of the bundle name and
          // can be used in chunks array of HtmlWebpackPlugin
          test: /[\\/]node_modules[\\/]/,
          chunks: "all",
        },
      }
      // cacheGroups: {
      //   // / node_modules /,
      //   // / bower_components /,
      //   // / packaged\/ public\/dist/,
      //   // /packaged\/public\/c/,
      //   // /packaged\/public\/common_dist/,
      //   // /lib/,
      //   // commons: {
      //   //   test: /[\\/]node_modules[\\/]/,
      //   //   name: 'common',
      //   //   chunks: 'all',
      //   //   reuseExistingChunk: true,
      //   // },
      //   materialui: {
      //     test: /[\\/]node_modules[\\/](@material-ui)[\\/]/,
      //     name: 'materialui',
      //     chunks: 'all',
      //     reuseExistingChunk: true,
      //   },
      //   react: {
      //     test: /[\\/]node_modules[\\/](react|react-dom)[\\/]/,
      //     name: 'react',
      //     chunks: 'all',
      //     reuseExistingChunk: true,
      //   },
      //   // common: {
      //   //   test: / [\\/](*)[\\/]components[\\/]/,
      //   //   chunks: "all",
      //   //   minSize: 0,
      //   // },
      //   components: {
      //     test: /[\\/]app[\\/]cdap[\\/]components[\\/]/,
      //     name: 'components',
      //     chunks: 'all',
      //   },
      //   services: {
      //     test: /[\\/]app[\\/]cdap[\\/]services[\\/]/,
      //     name: 'services',
      //     chunks: 'all',
      //   },
      //   api: {
      //     test: /[\\/]app[\\/]cdap[\\/]api[\\/]/,
      //     name: 'api',
      //     chunks: 'all',
      //   },
      //   wrangler: {
      //     test: /[\\/]app[\\/]cdap[\\/]wrangler[\\/]/,
      //     name: 'wrangler',
      //     chunks: 'all',
      //   },
      //   styles: {
      //     test: /[\\/]app[\\/]cdap[\\/]styles[\\/]/,
      //     name: 'styles',
      //     chunks: 'all',
      //   },
      //   // components: __dirname + '/app/cdap/components',
      //   // services: __dirname + '/app/cdap/services',
      //   // api: __dirname + '/app/cdap/api',
      //   // wrangler: __dirname + '/app/wrangler',
      //   // styles: __dirname + '/app/cdap/styles',
      //   bower: { test: /[\\/]bower_components[\\/]/, name: "bower", chunks: "all", reuseExistingChunk: true },
      //   dist: { test: /[\\/]packaged[\\/]public[\\/]dist[\\/]/, name: "dist", chunks: "all", reuseExistingChunk: true },
      //   //cdap_dist: { test: /[\\/]packaged[\\/]public[\\/]cdap_dist[\\/]/, name: "cdap_dist", chunks: "all", reuseExistingChunk: true },
      //   common_dist: { test: /[\\/]packaged[\\/]public[\\/]common_dist[\\/]/, name: "common_dist", chunks: "all", reuseExistingChunk: true },
      //   lib: { test: /[\\/]lib[\\/]/, name: "lib", chunks: "all", reuseExistingChunk: true },
      // }
    },
  },
  resolve: {
    extensions: ['.ts', '.tsx', '.js', '.jsx', '.scss'],
    alias: {
      components: __dirname + '/app/cdap/components',
      services: __dirname + '/app/cdap/services',
      api: __dirname + '/app/cdap/api',
      lib: __dirname + '/app/lib',
      styles: __dirname + '/app/cdap/styles',
    },
  },
};

if (!isModeProduction(mode)) {
  webpackConfig.devtool = 'source-maps';
}

if (isModeProduction(mode)) {
  webpackConfig.optimization.minimizer = [
    new TerserPlugin({
      terserOptions: {
        cache: false,
        parallel: true,
        sourceMap: true,
        extractComments: true,
        output: {
          comments: false,
        },
        ie8: false,
        safari10: false,
      },
    }),
  ];
}

module.exports = webpackConfig;
