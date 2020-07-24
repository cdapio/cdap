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
//var LiveReloadPlugin = require('webpack-livereload-plugin');
var HtmlWebpackPlugin = require('html-webpack-plugin');
var StyleLintPlugin = require('stylelint-webpack-plugin');
var CaseSensitivePathsPlugin = require('case-sensitive-paths-webpack-plugin');
var TerserPlugin = require('terser-webpack-plugin');
const { CleanWebpackPlugin } = require('clean-webpack-plugin');
var ForkTsCheckerWebpackPlugin = require('fork-ts-checker-webpack-plugin');
var LodashModuleReplacementPlugin = require('lodash-webpack-plugin');
const SpeedMeasurePlugin = require("speed-measure-webpack-plugin");
const UglifyJsPlugin = require('uglifyjs-webpack-plugin');
const MiniCssExtractPlugin = require('mini-css-extract-plugin');

const smp = new SpeedMeasurePlugin();

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
  //new webpack.HotModuleReplacementPlugin(),
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
    mode: isModeProduction(mode) ? '' : 'development.',
  }),
  new StyleLintPlugin({
    syntax: 'scss',
    files: ['**/*.scss'],
    lintDirtyModulesOnly: true,
  }),
];
if (!isModeProduction(mode)) {
  plugins.push(
    new ForkTsCheckerWebpackPlugin({
      tsconfig: __dirname + '/tsconfig.json',
      tslint: __dirname + '/tslint.json',
      tslintAutoFix: true,
      // watch: ["./app/cdap"], // optional but improves performance (less stat calls)
      memoryLimit: 8192,
    })
  );
}

var rules = [
  {
    test: /\.s?css$/,
    //use: ['style-loader', 'css-loader', 'postcss-loader', 'sass-loader'],
    use: ['style-loader', 'css-loader', 'postcss-loader', 'sass-loader'],
      // {
      //   loader: MiniCssExtractPlugin.loader,
      //   options: {
      //     hmr: !isModeProduction // since u know this is dev env
      //   }
      // },
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
  // {
  //   //test: /\.(j|t)sx?$/,
  //   test: /\.js$/,
  //   exclude: loaderExclude,
  //   include: [path.join(__dirname, 'app'), path.join(__dirname, '.storybook')],
  //   use: {
  //     loader: "babel-loader",
  //     options: {
  //       cacheDirectory: true,
  //       babelrc: false,
  //       presets: [
  //         [
  //           "@babel/preset-env",
  //           { targets: { browsers: [
  //             "last 10 versions",
  //             "safari >= 7"
  //           ] } }
  //         ],
  //         "@babel/preset-typescript",
  //         "@babel/preset-react"
  //       ],
  //       plugins: [
  //         // plugin-proposal-decorators is only needed if you're using experimental decorators in TypeScript
  //         ["@babel/plugin-proposal-decorators", { legacy: true }],
  //         ["@babel/plugin-proposal-class-properties", { loose: true }],
  //         "react-hot-loader/babel"
  //       ]
  //     }
  //   }
  // },
  {
    test: /\.js$/,
    use: ['react-hot-loader/webpack', 'babel-loader?cacheDirectory'],
    exclude: loaderExclude,
    include: [path.join(__dirname, 'app'), path.join(__dirname, '.storybook')],
    // plugins: ['react-hot-loader/babel'],
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
  // plugins.push(
  //   new LiveReloadPlugin({
  //     port: 35728,
  //     appendScriptTag: true,
  //     delay: 500,
  //     ignore:
  //       '/node_modules/|/bower_components/|/packaged/public/dist/|/packaged/public/cdap_dist/|/packaged/public/common_dist/|/lib/',
  //   })
  // );
}

var webpackConfig = {
  mode: isModeProduction(mode) ? 'production' : 'development',
  devtool: 'eval-source-map',
  context: __dirname + '/app/cdap',
  entry: {
    //cdap: ['webpack-dev-server/client?http://localhost:8080', 'webpack/hot/dev-server', '@babel/polyfill', './cdap.js'],
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
    //hotUpdateChunkFilename: 'hot-update.js',
    //hotUpdateMainFilename: 'hot-update.json',
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
  optimization: { // added
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
        // common: {
        //   name: 'common',
        //   minChunks: 2,
        //   chunks: 'async',
        //   priority: 10,
        //   reuseExistingChunk: true,
        //   enforce: true
        // }
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
    watchContentBase: true,
    historyApiFallback: true,
    hot: true,
    inline: true,
    compress: true,
    proxy: {
      '/api': {
        target: 'http://localhost:11011',
        pathRewrite: { '^/api': '' }
      }
    }
  }
};

if (!isModeProduction(mode)) {
  /// webpackConfig.devtool = 'cheap-eval-source-map'; // fixed
  webpackConfig.devtool = 'eval-cheap-module-source-map';
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
} else {
  const v8 = require('v8');
  console.log("hello", v8.getHeapStatistics().total_available_size / 1024 / 1024);
  webpackConfig.optimization.minimizer = [ // added
    // new UglifyJsPlugin({
    //   cache: true,
    //   parallel: true,
    // }),
    // new OptimizeCSSAssetsPlugin(), // doesn't seem to reduce
      // cssProcessorOptions: {
      //   parser: safePostCssParser,
      //   map: {
      //     inline: false,
      //     annotation: true
      //   }
      // }
    // )
  ]
}

//module.exports = webpackConfig;
module.exports = smp.wrap(webpackConfig);
