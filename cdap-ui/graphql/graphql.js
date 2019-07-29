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
const { queryTypeApplicationsResolver } = require('./Query/applicationsResolver');
const { queryTypeApplicationResolver } = require('./Query/applicationResolver');
const { queryTypeNamespacesResolver } = require('./Query/namespacesResolver');
const { queryTypeStatusResolver } = require('./Query/statusResolver');
const {
  applicationRecordTypeApplicationDetailResolver,
} = require('./types/ApplicationRecord/applicationDetailResolver');
const {
  applicationDetailTypeMetadataResolver,
} = require('./types/ApplicationDetail/metadataResolver');
const {
  applicationDetailTypeProgramsResolver,
} = require('./types/ApplicationDetail/programsResolver');
const {
  programRecordTypeResolveTypeResolver,
} = require('./types/ProgramRecord/resolveTypeResolver');
const { workflowTypeRunsResolver } = require('./types/Workflow/runsResolver');
const { workflowTypeSchedulesResolver } = require('./types/Workflow/schedulesResolver');
const { workflowTypeTotalRunsResolver } = require('./types/Workflow/totalRunsResolver');
const { sparkTypeRunsResolver } = require('./types/Spark/runsResolver');
const { sparkTypeTotalRunsResolver } = require('./types/Spark/totalRunsResolver');
const { mapReduceTypeRunsResolver } = require('./types/MapReduce/runsResolver');
const { mapReduceTypeTotalRunsResolver } = require('./types/MapReduce/totalRunsResolver');
const {
  scheduleDetailTypeNextRuntimesResolver,
} = require('./types/ScheduleDetail/nextRuntimesResolver');

const log = log4js.getLogger('graphql');
const env = process.env.NODE_ENV || 'production';
let rootSchemaPath;
/**
 * This will happen when we build SDK by running node server through ncc compiler.
 * This will flatten our directory structure and the __dirname will be cdap-sdk/ui
 * instead of cdap-sdk/ui/graphql. So when we start from sdk we need to append
 * graphql to the path for accessing schema.
 */
if (!__dirname.endsWith('/graphql')) {
  rootSchemaPath = path.join(__dirname, '/graphql/schema/rootSchema.graphql');
} else {
  rootSchemaPath = path.join(__dirname, '/schema/rootSchema.graphql');
}
let typeDefs = importSchema(rootSchemaPath);

if (typeof typeDefs === 'undefined') {
  const errorMessage = 'The GraphQL type definitions are undefined';
  log.error(errorMessage);
  throw new Error(errorMessage);
}

const resolvers = {
  Query: {
    applications: queryTypeApplicationsResolver,
    application: queryTypeApplicationResolver,
    namespaces: queryTypeNamespacesResolver,
    status: queryTypeStatusResolver,
  },
  ApplicationRecord: {
    applicationDetail: applicationRecordTypeApplicationDetailResolver,
  },
  ApplicationDetail: {
    metadata: applicationDetailTypeMetadataResolver,
    programs: applicationDetailTypeProgramsResolver,
  },
  ProgramRecord: { __resolveType: programRecordTypeResolveTypeResolver },
  Workflow: {
    runs: workflowTypeRunsResolver,
    schedules: workflowTypeSchedulesResolver,
    totalRuns: workflowTypeTotalRunsResolver,
  },
  Spark: {
    runs: sparkTypeRunsResolver,
    totalRuns: sparkTypeTotalRunsResolver,
  },
  MapReduce: {
    runs: mapReduceTypeRunsResolver,
    totalRuns: mapReduceTypeTotalRunsResolver,
  },
  ScheduleDetail: { nextRuntimes: scheduleDetailTypeNextRuntimesResolver },
};

const server = new ApolloServer({
  typeDefs,
  resolvers,
  context: ({ req }) => {
    if (!req || !req.headers || !req.headers.authorization) {
      return {};
    }

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
