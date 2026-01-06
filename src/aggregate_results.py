import pandas as pd
import sys
import os


def main():
    # check for command line argument
    if len(sys.argv) < 2:
        print("error: please provide input csv file path.")
        print("usage: python aggregate_results.py <path_to_results.csv>")
        sys.exit(1)

    file_path = sys.argv[1]

    # check if file exists
    if not os.path.exists(file_path):
        print(f"error: file '{file_path}' not found.")
        sys.exit(1)

    try:
        # load csv data
        df = pd.read_csv(file_path)

        # required columns check
        required_cols = ["query_type", "response_time_ms", "rows_returned"]
        if not all(col in df.columns for col in required_cols):
            print(f"error: input csv must contain columns: {required_cols}")
            sys.exit(1)

        # aggregation: group by query_type
        # calculate median for response time and take max for rows returned (should be constant per query)
        agg_df = (
            df.groupby("query_type")
            .agg({"response_time_ms": "median", "rows_returned": "max"})
            .reset_index()
        )

        # extract numeric part of query_type for natural sorting (q1, q2, ... q10)
        # otherwise q10 sorts before q2
        agg_df["sort_key"] = agg_df["query_type"].str.extract("(\d+)").astype(int)
        agg_df = agg_df.sort_values("sort_key")

        # cleanup: drop helper column and round time
        agg_df = agg_df.drop(columns=["sort_key"])
        agg_df["response_time_ms"] = agg_df["response_time_ms"].round(4)

        # output logic
        input_filename = os.path.basename(file_path)
        output_filename = f"aggr_{input_filename}"

        # 1. save to csv
        agg_df.to_csv(output_filename, index=False)
        print(f"success: aggregated results saved to '{output_filename}'\n")

        # 2. pretty print to console
        print("-" * 45)
        print(f"{'Query Type':<15} {'Median Time (ms)':<20} {'Rows':<5}")
        print("-" * 45)
        for _, row in agg_df.iterrows():
            print(
                f"{row['query_type']:<15} {row['response_time_ms']:<20} {row['rows_returned']:<5}"
            )
        print("-" * 45)

    except Exception as e:
        print(f"error processing file: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
