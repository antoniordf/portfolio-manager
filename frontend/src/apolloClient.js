import { ApolloClient, InMemoryCache } from "@apollo/client";

const client = new ApolloClient({
  uri: "/graphql/", // GraphQL endpoint
  cache: new InMemoryCache({
    typePolicies: {
      Query: {
        fields: {
          quadrantData: {
            // Define how quadrantData is cached
            merge(existing = [], incoming) {
              return incoming;
            },
          },
          stockTimeSeries: {
            // Define how stockTimeSeries is cached
            keyArgs: ["seriesId", "startDate", "endDate"],
            merge(existing = [], incoming) {
              return incoming;
            },
          },
        },
      },
    },
  }),
});

export default client;
