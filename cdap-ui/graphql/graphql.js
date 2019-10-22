/*
 * Copyright © 2019 Cask Data, Inc.
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

const path = require('path');
const { ApolloServer } = require('apollo-server-express');
const { importSchema } = require('graphql-import');
const log4js = require('log4js');
const { resolvers } = require('./resolvers');
const sessionToken = require('../server/token');
const { createLoaders } = require('./helpers/createLoaders');

const log = log4js.getLogger('graphql');
const env = process.env.NODE_ENV || 'production';

/**
 * This will happen when we build SDK by running node server through ncc compiler.
 * This will flatten our directory structure and the __dirname will be cdap-sdk/ui
 * instead of cdap-sdk/ui/graphql. So when we start from sdk we need to append
 * graphql to the path for accessing schema.
 */
let typeDefs;
if (!__dirname.endsWith('/graphql')) {
  typeDefs = importSchema(path.join(__dirname, '/graphql/schema/rootSchema.graphql'));
} else {
  typeDefs = importSchema(path.join(__dirname, '/schema/rootSchema.graphql'));
}

if (typeof typeDefs === 'undefined') {
  const errorMessage = 'The GraphQL type definitions are undefined';
  log.error(errorMessage);
  throw new Error(errorMessage);
}

const getApolloServer = (cdapConfig, logger = console) =>
  new ApolloServer({
    typeDefs,
    resolvers,
    context: ({ req }) => {
      if (!req || !req.headers || !req.headers.authorization) {
        return {
          loaders: createLoaders(),
        };
      }
      const sToken = req.headers['session-token'];
      const auth = req.headers.authorization;

      if (!sToken || (sToken && !sessionToken.validateToken(sToken, cdapConfig, logger, auth))) {
        throw new Error('Invalid Sesion Token');
      }

      return {
        auth,
        loaders: createLoaders(auth),
      };
    },
    introspection: env === 'production' ? false : true,
    playground: env === 'production' ? false : true,
  });

function applyMiddleware(app, cdapConfig, logger) {
  getApolloServer(cdapConfig, logger).applyMiddleware({ app });
}

module.exports = {
  applyMiddleware,
};
