/*
 * Copyright © 2019-2020 Cask Data, Inc.
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

import path from 'path';
import { ApolloServer } from 'apollo-server-express';
import log4js from 'log4js';
import { resolvers } from 'gql/resolvers';
import { importSchema } from 'graphql-import';
import * as sessionToken from 'server/token';
import { createLoaders } from 'gql/helpers/createLoaders';

const log = log4js.getLogger('graphql');
const env = process.env.NODE_ENV || 'production';

const typeDefs = importSchema(path.join(__dirname, '/graphql/schema/rootSchema.graphql'));

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
      let userIdValue, userIdProperty;
      if (cdapConfig['security.authentication.mode'] === 'PROXY') {
        userIdProperty = cdapConfig['security.authentication.proxy.user.identity.header'];
        userIdValue = req.headers[userIdProperty];
      }

      if (!sToken || (sToken && !sessionToken.validateToken(sToken, cdapConfig, logger, auth))) {
        throw new Error('Invalid Sesion Token');
      }

      return {
        auth,
        userIdProperty,
        userIdValue,
        loaders: createLoaders(auth, userIdProperty, userIdValue),
      };
    },
    introspection: env === 'production' ? false : true,
    playground: env === 'production' ? false : true,
  });

export function applyGraphQLMiddleware(app, cdapConfig, logger) {
  getApolloServer(cdapConfig, logger).applyMiddleware({ app });
}
