import { gql } from "@apollo/client";

export const GET_QUADRANT_DATA = gql`
  query GetQuadrantData($dataPoints: Int!) {
    quadrantData(dataPoints: $dataPoints) {
      date
      gdpGrowth
      inflationGrowth
    }
  }
`;
