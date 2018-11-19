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
var UglifyJsPlugin = require('uglifyjs-webpack-plugin');
const CleanWebpackPlugin = require('clean-webpack-plugin');
var ForkTsCheckerWebpackPlugin = require('fork-ts-checker-webpack-plugin');
var LodashModuleReplacementPlugin = require('lodash-webpack-plugin');
let pathsToClean = ['cdap_dist'];

// the clean options to use
let cleanOptions = {
  verbose: true,
  dry: false,
};

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
      context: path.resolve(__dirname, 'dll'),
      manifest: require(path.join(__dirname, 'dll', sharedDllManifestFileName)),
    }),
    new webpack.DllReferencePlugin({
      context: path.resolve(__dirname, 'dll'),
      manifest: require(path.join(__dirname, 'dll', cdapDllManifestFileName)),
    }),
  ];
};
var plugins = [
  new CleanWebpackPlugin(pathsToClean, cleanOptions),
  new CaseSensitivePathsPlugin(),
  ...getWebpackDllPlugins(mode),
  new LodashModuleReplacementPlugin({
    shorthands: true,
    collections: true,
    caching: true,
  }),
  new CaseSensitivePathsPlugin(),
  ...getWebpackDllPlugins(mode),
  new CopyWebpackPlugin([
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
  ]),
  new StyleLintPlugin({
    syntax: 'scss',
    files: ['**/*.scss'],
  }),
  new HtmlWebpackPlugin({
    title: 'CDAP',
    template: './cdap.html',
    filename: 'cdap.html',
    hash: true,
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
    test: /\.scss$/,
    use: ['style-loader', 'css-loader', 'postcss-loader', 'sass-loader'],
  },
  {
    test: /\.ya?ml$/,
    use: 'yml-loader',
  },
  {
    test: /\.css$/,
    use: ['style-loader', 'css-loader', 'postcss-loader', 'sass-loader'],
  },
  {
    enforce: 'pre',
    test: /\.js$/,
    loader: 'eslint-loader',
    options: {
      fix: true,
    },
    exclude: [
      /node_modules/,
      /bower_components/,
      /dist/,
      /old_dist/,
      /cdap_dist/,
      /common_dist/,
      /lib/,
      /wrangler_dist/,
    ],
  },
  {
    test: /\.js$/,
    use: ['babel-loader'],
    exclude: [/node_modules/, /lib/],
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
        },
      },
    ],
    exclude: [/node_modules/, /lib/],
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
    }),
    new UglifyJsPlugin({
      uglifyOptions: {
        ie8: false,
        compress: {
          warnings: false,
        },
        output: {
          comments: false,
          beautify: false,
        },
      },
    })
  );
}

if (mode === 'development') {
  plugins.push(
    new LiveReloadPlugin({
      port: 35728,
      appendScriptTag: true,
    })
  );
}

var webpackConfig = {
  mode: isModeProduction(mode) ? 'production' : 'development',
  devtool: 'source-map',
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
    path: __dirname + '/cdap_dist/cdap_assets/',
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
    splitChunks: false,
  },
  resolve: {
    extensions: ['.ts', '.tsx', '.js', '.jsx'],
    alias: {
      components: __dirname + '/app/cdap/components',
      services: __dirname + '/app/cdap/services',
      api: __dirname + '/app/cdap/api',
      lib: __dirname + '/app/lib',
      styles: __dirname + '/app/cdap/styles',
    },
  },
};

module.exports = webpackConfig;
