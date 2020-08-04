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
var HtmlWebpackPlugin = require('html-webpack-plugin');
var StyleLintPlugin = require('stylelint-webpack-plugin');
var CaseSensitivePathsPlugin = require('case-sensitive-paths-webpack-plugin');
const { CleanWebpackPlugin } = require('clean-webpack-plugin');
var ForkTsCheckerWebpackPlugin = require('fork-ts-checker-webpack-plugin');
var LodashModuleReplacementPlugin = require('lodash-webpack-plugin');

const devMode = process.env.NODE_ENV;

// the clean options to use
let cleanOptions = {
  verbose: true,
  dry: false,
  cleanStaleWebpackAssets: false, // reduces down from 2.7seconds to 2.2seconds
};

const loaderExclude = [
  /node_modules/,
  /bower_components/,
  /packaged\/public\/dist/,
  /packaged\/public\/cdap_dist/,
  /packaged\/public\/common_dist/,
  /lib/,
];

var mode = 'development'; // process.env.NODE_ENV || 'production';
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
      copyUnmodified: false,
    }
  ),
  new HtmlWebpackPlugin({
    title: 'CDAP',
    template: './cdap.html',
    filename: 'cdap.html',
    hash: true,
    inject: false,
    mode: 'development.',
  }),
  new StyleLintPlugin({
    syntax: 'scss',
    files: ['**/*.scss'],
    lintDirtyModulesOnly: true,
  }),
  new ForkTsCheckerWebpackPlugin({
    tsconfig: __dirname + '/tsconfig.json',
    tslint: __dirname + '/tslint.json',
    tslintAutoFix: true,
    // watch: ["./app/cdap"], // optional but improves performance (less stat calls)
    memoryLimit: 8192,
  })
];
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
    use: ['react-hot-loader/webpack', 'babel-loader?cacheDirectory'],
    exclude: loaderExclude,
    include: [path.join(__dirname, 'app'), path.join(__dirname, '.storybook')],
  },
  {
    test: /\.tsx?$/,
    use: [
      'react-hot-loader/webpack',
      'babel-loader?cacheDirectory',
      {
        loader: 'ts-loader',
        options: {
          transpileOnly: true,
          experimentalWatchApi: true, // added
        },
      },
    ],
    exclude: loaderExclude,
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

var webpackConfig = {
  mode: 'development',
  devtool: 'eval-cheap-module-source-map',
  context: __dirname + '/app/cdap',
  entry: {
    cdap: [
      'react-hot-loader/patch', // RHL patch
      '@babel/polyfill',
      './cdap.js'
    ],
  },
  module: {
    rules,
  },
  output: {
    filename: '[name].js',
    chunkFilename: '[name].js',
    hotUpdateChunkFilename: 'hot-update.js',
    hotUpdateMainFilename: 'hot-update.json',
    path: __dirname + '/packaged/public/cdap_dist/cdap_assets/',
    publicPath: '/cdap_assets/',
    pathinfo: false, // added. reduces 0.2~0.3 seconds
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
    removeAvailableModules: false,
    removeEmptyChunks: false,
    splitChunks: {
      cacheGroups: {
        vendor: {
          name: "node_vedors",
          test: /[\\/]node_modules[\\/]/,
          chunks: "all",
          reuseExistingChunk: true,
        },
        fonts: {
          name: "fonts",
          test: /[\\/]app[\\/]cdap[\\/]styles[\\/]fonts[\\/]/,
          chunks: "all",
          reuseExistingChunk: true,
        },
      }
    },
    minimize: true,
  },
  resolve: {
    extensions: ['.ts', '.tsx', '.js', '.jsx', '.scss'],
    alias: {
      components: __dirname + '/app/cdap/components',
      services: __dirname + '/app/cdap/services',
      api: __dirname + '/app/cdap/api',
      lib: __dirname + '/app/lib',
      styles: __dirname + '/app/cdap/styles',
      'react-dom': '@hot-loader/react-dom',
    },
  },
  devServer: {
    index: 'cdap.html',
    contentBase: path.join(__dirname, '/packaged/public/cdap_dist/cdap_assets/'),
    port: 8080,
    open: 'chrome',
    writeToDisk: true,
    publicPath: '/cdap_assets/',
    watchContentBase: devMode === 'reload',
    historyApiFallback: true,
    hot: true,
    inline: true,
    proxy: {
      '/api': {
        target: 'http://localhost:11011',
        pathRewrite: { '^/api': '' }
      }
    }
  }
};

const v8 = require('v8');
console.log("Allocated ", v8.getHeapStatistics().total_available_size / 1024 / 1024);
webpackConfig.optimization.minimizer = [];

module.exports = webpackConfig;
