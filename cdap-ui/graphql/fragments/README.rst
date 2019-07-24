# Heuristic Fragment Matcher Warning

## Introduction

As of July of 2017, querying unions, fragments, and interfaces with Apollo Client produces an error that prevents obtaining a result. In order to fix this error, the Apollo Client must be passed information about unions, fragments, and interfaces. The following sections describe how to fix the issue.

## Solution

The following pages describe the above problem and how to fix it:

- [Apollo Client documentation - Fragments on unions and interfaces](https://www.apollographql.com/docs/react/advanced/fragments/#fragments-on-unions-and-interfaces).
- [Heuristic Fragment matcher warning! How to fix it?](https://medium.com/commutatus/whats-going-on-with-the-heuristic-fragment-matcher-in-graphql-apollo-client-e721075e92be).

The script `./graphql/schemaQuery.js` fetches the fragments from the GraphQL schema and saves them in `./graphql/fragments/fragmentTypes.json`. This file is then imported and passed to the Apollo Client. **Do not modify the json file yourself**. The following command will generate the file for you.

### Command

The following command should be executed every time there is an update the schema

`yarn build-graphql-fragment`

In order to fetch the fragments, the GraphQL Server must be running. The server is started when the CDAP UI is started, which is accomplished by running locally `yarn start`.