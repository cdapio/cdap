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

const { ApolloServer } = require('apollo-server-express');
const { importSchema } = require('graphql-import');
const log4js = require('log4js');
const { applicationDetailResolver } = require('./resolvers/applicationResolvers');
const { metadataResolver } = require('./resolvers/metadataResolvers');
const { programsResolver } = require('./resolvers/programRecordResolvers');
const { programsTypeResolver } = require('./resolvers/type/programRecordTypeResolver');
const { schedulesResolver, nextRuntimesResolver } = require('./resolvers/scheduleResolvers');
const queryType = require('./types/query');

const log = log4js.getLogger('graphql');
const env = process.env.NODE_ENV || 'production';

let typeDefs;

if (env === 'production') {
  typeDefs = importSchema('./ui/graphql/schema/rootSchema.graphql');
} else if (env === 'development') {
  typeDefs = importSchema('./graphql/schema/rootSchema.graphql');
}

if (typeof typeDefs === 'undefined') {
  const errorMessage = 'The GraphQL type definitions are undefined';
  log.error(errorMessage);
  throw new Error(errorMessage);
}

const resolvers = {
  Query: {
    applications: queryType.applicationsResolver,
    application: queryType.applicationResolver,
    namespaces: queryType.namespacesResolver,
    status: queryType.statusResolver,
  },
  ApplicationRecord: {
    applicationDetail: applicationDetailResolver.ApplicationRecord.applicationDetail,
  },
  ApplicationDetail: {
    metadata: metadataResolver.ApplicationDetail.metadata,
    programs: programsResolver.ApplicationDetail.programs,
  },
  ProgramRecord: { __resolveType: programsTypeResolver.ProgramRecord.__resolveType },
  Workflow: {
    runs: programsTypeResolver.Workflow.runs,
    schedules: schedulesResolver.Workflow.schedules,
  },
  Spark: {
    runs: programsTypeResolver.Spark.runs,
  },
  MapReduce: {
    runs: programsTypeResolver.MapReduce.runs,
  },
  ScheduleDetail: { nextRuntimes: nextRuntimesResolver.ScheduleDetail.nextRuntimes },
};

const server = new ApolloServer({
  typeDefs,
  resolvers,
  context: ({ req }) => {
    const auth = req.headers.authorization;

    return { auth };
  },
  introspection: env === 'production' ? false : true,
  playground: env === 'production' ? false : true,
});

function applyMiddleware(app) {
  server.applyMiddleware({ app });
}

module.exports = {
  applyMiddleware,
  resolvers,
  typeDefs,
};
