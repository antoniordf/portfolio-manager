import { gql } from "@apollo/client";

export const GET_STOCK_TIME_SERIES = gql`
  query GetStockTimeSeries(
    $seriesId: String!
    $startDate: Date
    $endDate: Date
  ) {
    stockTimeSeries(
      seriesId: $seriesId
      startDate: $startDate
      endDate: $endDate
    ) {
      date
      open
      high
      low
      close
      volume
    }
  }
`;
