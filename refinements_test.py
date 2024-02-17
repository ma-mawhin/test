# import src.utils.refinements_helpers as rh
# from pyspark.sql.types import IntegerType, StringType, DateType
# from datetime import date


# def test_get_worker_id_table(spark_session):
#     data = [
#         (1, "Reader1", "2023-07-25", "10:30:00", 10),
#     ]
#     df_columns = ["id_worker", "readerdesc", "date", "time", "hour"]
#     df = spark_session.createDataFrame(data, df_columns)

#     csv_data = [
#         (1, "Division1", "Group1", "Team1", 5),
#     ]
#     csv_columns = ["worker_id", "division", "groupe", "team", "desks"]
#     csv_df = spark_session.createDataFrame(csv_data, csv_columns)

#     output_df = rh.get_worker_id_table(df, csv_df)

#     expected_schema = {
#         "worker_id": IntegerType(),
#         "readerdesc": StringType(),
#         "date": StringType(),
#         "time": StringType(),
#         "hour": IntegerType(),
#         "division": StringType(),
#         "groupe": StringType(),
#         "team": StringType(),
#         "desks": IntegerType(),
#     }

#     for column_name, data_type in expected_schema.items():
#         assert output_df.schema[column_name].dataType == data_type


# def test_calculate_occupation(spark_session):
#     data = [
#         (
#             1,
#             "Reader1",
#             "10:30:00",
#             10,
#             "Division1",
#             "Groupe1",
#             "Team1",
#             10,
#             "2023-07-25",
#             2023,
#             "P1",
#             "W1",
#         ),
#         (
#             2,
#             "Reader1",
#             "11:30:00",
#             11,
#             "Division2",
#             "Groupe1",
#             "Team1",
#             10,
#             "2023-07-25",
#             2023,
#             "P1",
#             "W1",
#         ),
#         (
#             3,
#             "Reader1",
#             "08:30:00",
#             8,
#             "Division1",
#             "Groupe3",
#             "Team1",
#             10,
#             "2023-07-25",
#             2023,
#             "P1",
#             "W1",
#         ),
#         (
#             4,
#             "Reader1",
#             "10:30:00",
#             10,
#             "Division1",
#             "Groupe1",
#             "Team4",
#             10,
#             "2023-07-25",
#             2023,
#             "P1",
#             "W1",
#         ),
#         (
#             5,
#             "Reader1",
#             "10:30:00",
#             10,
#             "Division1",
#             "Groupe1",
#             "Team1",
#             10,
#             "2023-07-24",
#             2023,
#             "P1",
#             "W1",
#         ),
#         (
#             6,
#             "Reader1",
#             "10:30:00",
#             10,
#             "Division1",
#             "Groupe1",
#             "Team1",
#             10,
#             "2023-07-24",
#             2023,
#             "P1",
#             "W1",
#         ),
#     ]
#     df_columns = [
#         "worker_id",
#         "readerdesc",
#         "time",
#         "hour",
#         "division",
#         "groupe",
#         "team",
#         "desks",
#         "date",
#         "year",
#         "periods",
#         "week",
#     ]
#     df = spark_session.createDataFrame(data, df_columns)

#     value = 5
#     df_result, output_df_result = rh.calculate_occupation(df, value)

#     expected_df_data = [
#         (
#             2,
#             0.4,
#             5,
#             "Reader1",
#             "10:30:00",
#             10,
#             "Division1",
#             "Groupe1",
#             "Team1",
#             10,
#             "2023-07-24",
#             2023,
#             "P1",
#             "W1",
#         ),
#         (
#             2,
#             0.4,
#             6,
#             "Reader1",
#             "10:30:00",
#             10,
#             "Division1",
#             "Groupe1",
#             "Team1",
#             10,
#             "2023-07-24",
#             2023,
#             "P1",
#             "W1",
#         ),
#         (
#             4,
#             0.8,
#             1,
#             "Reader1",
#             "10:30:00",
#             10,
#             "Division1",
#             "Groupe1",
#             "Team1",
#             10,
#             "2023-07-25",
#             2023,
#             "P1",
#             "W1",
#         ),
#         (
#             4,
#             0.8,
#             2,
#             "Reader1",
#             "11:30:00",
#             11,
#             "Division2",
#             "Groupe1",
#             "Team1",
#             10,
#             "2023-07-25",
#             2023,
#             "P1",
#             "W1",
#         ),
#         (
#             4,
#             0.8,
#             3,
#             "Reader1",
#             "08:30:00",
#             8,
#             "Division1",
#             "Groupe3",
#             "Team1",
#             10,
#             "2023-07-25",
#             2023,
#             "P1",
#             "W1",
#         ),
#         (
#             4,
#             0.8,
#             4,
#             "Reader1",
#             "10:30:00",
#             10,
#             "Division1",
#             "Groupe1",
#             "Team4",
#             10,
#             "2023-07-25",
#             2023,
#             "P1",
#             "W1",
#         ),
#     ]
#     expected_df_columns = [
#         "count_by_date_worker_id",
#         "occupation_total",
#         "HQ_associates",
#         "readerdesc",
#         "time",
#         "hour",
#         "division",
#         "groupe",
#         "team",
#         "desks",
#         "date",
#         "year",
#         "periods",
#         "week",
#     ]
#     expected_df = spark_session.createDataFrame(expected_df_data, expected_df_columns)

#     expected_output_df_data = [
#         ("2023-07-24", "Division1", 0.2),
#         ("2023-07-25", "Division1", 0.3),
#         ("2023-07-25", "Division2", 0.1),
#     ]
#     expected_output_df_columns = ["event_date", "temp_division", "desks_occupation"]
#     expected_output_df = spark_session.createDataFrame(
#         expected_output_df_data, expected_output_df_columns
#     )

#     assert df_result.collect() == expected_df.collect()
#     assert output_df_result.collect() == expected_output_df.collect()


# def test_get_presence_overview(spark_session):
#     data = [
#         (
#             4,
#             0.8,
#             1,
#             "Reader1",
#             "10:30:00",
#             10,
#             "Division1",
#             "Groupe1",
#             "Team1",
#             10,
#             "2023-07-25",
#             2023,
#             "P1",
#             "W1",
#         ),
#         (
#             4,
#             0.8,
#             2,
#             "Reader1",
#             "11:30:00",
#             11,
#             "Division2",
#             "Groupe1",
#             "Team1",
#             10,
#             "2023-07-25",
#             2023,
#             "P1",
#             "W1",
#         ),
#         (
#             4,
#             0.8,
#             3,
#             "Reader1",
#             "08:30:00",
#             8,
#             "Division1",
#             "Groupe3",
#             "Team1",
#             10,
#             "2023-07-25",
#             2023,
#             "P1",
#             "W1",
#         ),
#         (
#             4,
#             0.8,
#             4,
#             "Reader1",
#             "10:30:00",
#             10,
#             "Division1",
#             "Groupe1",
#             "Team4",
#             10,
#             "2023-07-25",
#             2023,
#             "P1",
#             "W1",
#         ),
#         (
#             2,
#             0.4,
#             5,
#             "Reader1",
#             "10:30:00",
#             10,
#             "Division1",
#             "Groupe1",
#             "Team1",
#             10,
#             "2023-07-24",
#             2023,
#             "P1",
#             "W1",
#         ),
#         (
#             2,
#             0.4,
#             6,
#             "Reader1",
#             "10:30:00",
#             10,
#             "Division1",
#             "Groupe1",
#             "Team1",
#             10,
#             "2023-07-24",
#             2023,
#             "P1",
#             "W1",
#         ),
#     ]
#     df_columns = [
#         "count_by_date_worker_id",
#         "occupation_total",
#         "HQ_associates",
#         "readerdesc",
#         "time",
#         "hour",
#         "division",
#         "groupe",
#         "team",
#         "desks",
#         "date",
#         "year",
#         "periods",
#         "week",
#     ]
#     df = spark_session.createDataFrame(data, df_columns)

#     desks_df_data = [
#         ("2023-07-25", "Division1", 0.3),
#         ("2023-07-25", "Division2", 0.1),
#         ("2023-07-24", "Division1", 0.2),
#     ]

#     desks_df_columns = ["event_date", "temp_division", "desks_occupation"]
#     desks_df = spark_session.createDataFrame(desks_df_data, desks_df_columns)

#     desks_df_result, df_result = rh.get_presence_overview(df, desks_df)

#     expected_df_data = [
#         (
#             0.2,
#             2,
#             0.4,
#             5,
#             "Reader1",
#             "10:30:00",
#             10,
#             "Division1",
#             "Groupe1",
#             "Team1",
#             10,
#             "2023-07-24",
#             2023,
#             "P1",
#             "W1",
#         ),
#         (
#             0.2,
#             2,
#             0.4,
#             6,
#             "Reader1",
#             "10:30:00",
#             10,
#             "Division1",
#             "Groupe1",
#             "Team1",
#             10,
#             "2023-07-24",
#             2023,
#             "P1",
#             "W1",
#         ),
#         (
#             0.3,
#             4,
#             0.8,
#             1,
#             "Reader1",
#             "10:30:00",
#             10,
#             "Division1",
#             "Groupe1",
#             "Team1",
#             10,
#             "2023-07-25",
#             2023,
#             "P1",
#             "W1",
#         ),
#         (
#             0.3,
#             4,
#             0.8,
#             3,
#             "Reader1",
#             "08:30:00",
#             8,
#             "Division1",
#             "Groupe3",
#             "Team1",
#             10,
#             "2023-07-25",
#             2023,
#             "P1",
#             "W1",
#         ),
#         (
#             0.3,
#             4,
#             0.8,
#             4,
#             "Reader1",
#             "10:30:00",
#             10,
#             "Division1",
#             "Groupe1",
#             "Team4",
#             10,
#             "2023-07-25",
#             2023,
#             "P1",
#             "W1",
#         ),
#         (
#             0.1,
#             4,
#             0.8,
#             2,
#             "Reader1",
#             "11:30:00",
#             11,
#             "Division2",
#             "Groupe1",
#             "Team1",
#             10,
#             "2023-07-25",
#             2023,
#             "P1",
#             "W1",
#         ),
#     ]
#     expected_df_columns = [
#         "desks_occupation",
#         "count_by_date_worker_id",
#         "occupation_total",
#         "HQ_associates",
#         "readerdesc",
#         "time",
#         "hour",
#         "division",
#         "groupe",
#         "team",
#         "desks",
#         "date",
#         "year",
#         "periods",
#         "week",
#     ]
#     expected_df = spark_session.createDataFrame(expected_df_data, expected_df_columns)

#     expected_overview_data = [
#         ("2023-07-25", 0.8),
#     ]
#     expected_overview_columns = ["date", "occupation_total"]
#     expected_overview_df = spark_session.createDataFrame(
#         expected_overview_data, expected_overview_columns
#     )

#     assert df_result.collect() == expected_df.collect()
#     assert desks_df_result.collect() == expected_overview_df.collect()


# def test_calculate_entries(spark_session):
#     data = [
#         (1, "L06-000 Entrée Barrière", "2023-07-25", "10:30:00", 10),
#         (2, "L06-000 Entrée Barrière", "2023-07-23", "12:20:00", 12),
#         (3, "Reader1", "2023-07-23", "11:30:00", 11),
#         (4, "Reader2", "2023-07-24", "09:30:00", 9),
#         (5, "L06-000 Entrée Barrière", "2023-07-25", "10:30:00", 10),
#     ]
#     df_columns = ["id_worker", "readerdesc", "date", "time", "hour"]
#     df = spark_session.createDataFrame(data, df_columns)

#     df_result = rh.calculate_entries(df)

#     expected_df_data = [
#         ("2023-07-25", 10, 2),
#         ("2023-07-23", 12, 1),
#     ]
#     expected_df_columns = ["date", "hour", "entries"]
#     expected_df = spark_session.createDataFrame(expected_df_data, expected_df_columns)

#     assert df_result.collect() == expected_df.collect()


# def test_calculate_exits(spark_session):
#     data = [
#         (1, "L06-001 Sortie Barrière", "2023-07-25", "10:30:00", 10),
#         (2, "L06-001 Sortie Barrière", "2023-07-23", "12:20:00", 12),
#         (3, "Reader1", "2023-07-23", "11:30:00", 11),
#         (4, "Reader2", "2023-07-24", "09:30:00", 9),
#         (5, "L06-001 Sortie Barrière", "2023-07-25", "10:30:00", 10),
#     ]
#     df_columns = ["id_worker", "readerdesc", "date", "time", "hour"]
#     df = spark_session.createDataFrame(data, df_columns)

#     df_result = rh.calculate_exits(df)

#     expected_df_data = [
#         ("2023-07-25", 10, 2),
#         ("2023-07-23", 12, 1),
#     ]
#     expected_df_columns = ["date", "hour", "exits"]
#     expected_df = spark_session.createDataFrame(expected_df_data, expected_df_columns)

#     assert df_result.collect() == expected_df.collect()


# def test_calculate_fill_percentage(spark_session):
#     entries_data = [
#         ("2023-07-25", 10, 7),
#         ("2023-07-25", 12, 13),
#         ("2023-07-23", 12, 1),
#     ]
#     entries_columns = ["date", "hour", "entries"]
#     entries_df = spark_session.createDataFrame(entries_data, entries_columns)

#     exits_data = [
#         ("2023-07-25", 10, 5),
#         ("2023-07-23", 12, 1),
#     ]
#     exits_columns = ["date", "hour", "exits"]
#     exits_df = spark_session.createDataFrame(exits_data, exits_columns)
#     value = 35

#     df_result = rh.calculate_fill_percentage(entries_df, exits_df, value)

#     expected_df_data = [
#         ("2023-07-23", 12, 1, 1, 0.0),
#         ("2023-07-25", 10, 7, 5, 0.0571),
#         ("2023-07-25", 12, 13, 0, 0.3714),
#     ]
#     expected_df_columns = ["date", "hour", "entries", "exits", "fill_percentage"]
#     expected_df = spark_session.createDataFrame(expected_df_data, expected_df_columns)

#     assert df_result.collect() == expected_df.collect()


# def test_calculate_cumulative_fill_percentage(spark_session):
#     data = [
#         ("2023-07-23", 7, 1, 0, 0.0285),
#         ("2023-07-23", 9, 15, 7, 0.2285),
#         ("2023-07-25", 10, 7, 5, 0.0571),
#         ("2023-07-25", 12, 13, 0, 0.3714),
#     ]
#     df_columns = ["date", "hour", "entries", "exits", "fill_percentage"]
#     df = spark_session.createDataFrame(data, df_columns)

#     df_result = rh.calculate_cumulative_fill_percentage(df)

#     expected_df_data = [
#         ("2023-07-23", 7, 1, 0, 0.0285, 0.0285),
#         ("2023-07-23", 9, 15, 7, 0.2285, 0.2570),
#         ("2023-07-25", 10, 7, 5, 0.0571, 0.0571),
#         ("2023-07-25", 12, 13, 0, 0.3714, 0.4285),
#     ]
#     expected_df_columns = [
#         "date",
#         "hour",
#         "entries",
#         "exits",
#         "fill_percentage",
#         "cumulative_fill_percentage",
#     ]
#     expected_df = spark_session.createDataFrame(expected_df_data, expected_df_columns)

#     assert df_result.collect() == expected_df.collect()


# def test_sort_fill_percentage(spark_session):
#     data = [
#         ("2023-07-25", 12, 13, 0, 0.3714, 0.4285),
#         ("2023-07-25", 10, 7, 5, 0.0571, 0.0571),
#         ("2023-07-23", 9, 15, 7, 0.2285, 0.2570),
#         ("2023-07-23", 7, 1, 0, 0.0285, 0.0285),
#     ]
#     df_columns = [
#         "date",
#         "hour",
#         "entries",
#         "exits",
#         "fill_percentage",
#         "cumulative_fill_percentage",
#     ]
#     df = spark_session.createDataFrame(data, df_columns)

#     df_result = rh.sort_fill_percentage(df)

#     expected_df_data = [
#         ("2023-07-25", 12, 0.3714, 0.4285),
#         ("2023-07-25", 10, 0.0571, 0.0571),
#         ("2023-07-23", 9, 0.2285, 0.2570),
#         ("2023-07-23", 7, 0.0285, 0.0285),
#     ]
#     expected_df_columns = [
#         "date",
#         "hour",
#         "fill_percentage",
#         "cumulative_fill_percentage",
#     ]
#     expected_df = spark_session.createDataFrame(expected_df_data, expected_df_columns)

#     assert df_result.collect() == expected_df.collect()


# def test_add_row_number_column(spark_session):
#     data = [
#         ("2023-07-25", 12, 0.3714, 0.4285),
#         ("2023-07-25", 10, 0.0571, 0.0571),
#         ("2023-07-23", 9, 0.2285, 0.2570),
#         ("2023-07-23", 7, 0.0285, 0.0285),
#     ]
#     df_columns = ["date", "hour", "fill_percentage", "cumulative_fill_percentage"]
#     df = spark_session.createDataFrame(data, df_columns)

#     column_name = "cumulative_fill_percentage"
#     df_result = rh.add_row_number_column(df, column_name)

#     expected_data = [
#         ("2023-07-23", 9, 0.2285, 0.2570, 1),
#         ("2023-07-23", 7, 0.0285, 0.0285, 2),
#         ("2023-07-25", 12, 0.3714, 0.4285, 1),
#         ("2023-07-25", 10, 0.0571, 0.0571, 2),
#     ]
#     expected_columns = [
#         "date",
#         "hour",
#         "fill_percentage",
#         "cumulative_fill_percentage",
#         "row_num",
#     ]
#     expected_df = spark_session.createDataFrame(expected_data, expected_columns)

#     assert df_result.collect() == expected_df.collect()


# def test_calculate_avg_fill_percentage(spark_session):
#     data = [
#         ("2023-07-25", 12, 0.3714, 0.4285),
#         ("2023-07-25", 10, 0.0571, 0.0571),
#         ("2023-07-23", 9, 0.2285, 0.2570),
#         ("2023-07-23", 7, 0.0285, 0.0285),
#     ]
#     df_columns = ["date", "hour", "fill_percentage", "cumulative_fill_percentage"]
#     df = spark_session.createDataFrame(data, df_columns)

#     df_result = rh.calculate_avg_fill_percentage(df)

#     expected_data = [
#         ("2023-07-25", 0.2428),
#         ("2023-07-23", 0.1428),
#     ]
#     expected_columns = ["date", "avg_fill_percentage"]
#     expected_df = spark_session.createDataFrame(expected_data, expected_columns)

#     assert df_result.collect() == expected_df.collect()


# def test_get_max_fill_percentage_per_day_hour(spark_session):
#     data = [
#         ("2023-07-25", 12, 0.3714, 0.4285),
#         ("2023-07-25", 10, 0.0571, 0.0571),
#         ("2023-07-23", 9, 0.2285, 0.2570),
#         ("2023-07-23", 7, 0.0285, 0.0285),
#     ]
#     df_columns = ["date", "hour", "fill_percentage", "cumulative_fill_percentage"]
#     df = spark_session.createDataFrame(data, df_columns)

#     df_result = rh.get_max_fill_percentage_per_day_hour(df)

#     expected_data = [
#         ("2023-07-25", 12, 0.4285, 0.2428),
#         ("2023-07-23", 9, 0.2570, 0.1428),
#     ]
#     expected_columns = [
#         "event_date",
#         "temp_hour",
#         "max_fill_percentage",
#         "avg_fill_percentage",
#     ]
#     expected_df = spark_session.createDataFrame(expected_data, expected_columns)

#     assert df_result.collect() == expected_df.collect()


# def test_get_agg_parking_table(spark_session):
#     data = [
#         ("2023-07-25", 12, 0.3714, 0.4285),
#         ("2023-07-25", 10, 0.0571, 0.0571),
#         ("2023-07-23", 9, 0.2285, 0.2570),
#         ("2023-07-23", 7, 0.0285, 0.0285),
#     ]
#     df_columns = ["date", "hour", "fill_percentage", "cumulative_fill_percentage"]
#     df = spark_session.createDataFrame(data, df_columns)

#     date_data = [
#         ("2023-07-25", 2022, "P2", "W1"),
#         ("2023-07-23", 2022, "P1", "W4"),
#     ]
#     date_df_columns = ["event_date", "year", "periods", "week"]
#     date_df = spark_session.createDataFrame(date_data, date_df_columns)

#     df_result, df_result_overview = rh.get_agg_parking_table(df, date_df)

#     expected_data = [
#         ("12:00:00 PM", 0.4285, 0.4285, 0.2428, "2023-07-25", 2022, "P2", "W1"),
#         ("10:00:00 AM", 0.0571, 0.4285, 0.2428, "2023-07-25", 2022, "P2", "W1"),
#         ("09:00:00 AM", 0.2570, 0.2570, 0.1428, "2023-07-23", 2022, "P1", "W4"),
#         ("07:00:00 AM", 0.0285, 0.2570, 0.1428, "2023-07-23", 2022, "P1", "W4"),
#     ]
#     expected_columns = [
#         "hour",
#         "cumulative_fill_percentage",
#         "max_fill_percentage",
#         "avg_fill_percentage",
#         "date",
#         "year",
#         "periods",
#         "week",
#     ]
#     expected_df = spark_session.createDataFrame(expected_data, expected_columns)

#     overview_expected_data = [
#         ("2023-07-25", 0.4285),
#     ]
#     overview_expected_columns = ["date", "max_fill_percentage"]
#     overview_expected_df = spark_session.createDataFrame(
#         overview_expected_data, overview_expected_columns
#     )

#     assert df_result.collect() == expected_df.collect()
#     assert df_result_overview.collect() == overview_expected_df.collect()


# def test_extract_date_and_time(spark_session):
#     data = [
#         ("24/04/2023 11:33:31", "Usine Aimargues", 739847703300, 1001002, 1),
#         ("24/04/2023 11:37:41", "Royal Canin Siege", 411414782980, 1001002, 2),
#         ("26/04/2023 12:07:18", "Royal Canin Siege", 983049251588, 1001002, 27),
#     ]
#     df_columns = ["ticket_date", "hierarchy_n1", "badge", "device", "ticket_number"]
#     df = spark_session.createDataFrame(data, df_columns)

#     df_result = rh.extract_date_and_time(df)

#     expected_data = [
#         ("Usine Aimargues", 739847703300, 1001002, 1, date(2023, 4, 24), "11:33:31"),
#         ("Royal Canin Siege", 411414782980, 1001002, 2, date(2023, 4, 24), "11:37:41"),
#         ("Royal Canin Siege", 983049251588, 1001002, 27, date(2023, 4, 26), "12:07:18"),
#     ]
#     expected_columns = [
#         "hierarchy_n1",
#         "badge",
#         "device",
#         "ticket_number",
#         "date",
#         "hour",
#     ]
#     expected_df = spark_session.createDataFrame(expected_data, expected_columns)

#     assert df_result.collect() == expected_df.collect()


# def test_calculate_mean_meals_per_weekday(spark_session):
#     data = [
#         ("Royal Canin Siege", 983049251588, 1001002, 27, date(2023, 4, 19), "12:07:18"),
#         ("Usine Aimargues", 739847703300, 1001002, 1, date(2023, 4, 19), "11:33:31"),
#         ("Royal Canin Siege", 411414782980, 1001002, 2, date(2023, 4, 19), "11:37:41"),
#         ("Royal Canin Siege", 983049251588, 1001002, 27, date(2023, 4, 19), "12:07:18"),
#         ("Usine Aimargues", 739847703300, 1001002, 1, date(2023, 4, 24), "11:33:31"),
#         ("Royal Canin Siege", 411414782980, 1001002, 2, date(2023, 4, 24), "11:37:41"),
#         ("Royal Canin Siege", 983049251588, 1001002, 27, date(2023, 4, 26), "12:07:18"),
#         ("Usine Aimargues", 739847703300, 1001002, 1, date(2023, 4, 26), "11:33:31"),
#         ("Royal Canin Siege", 411414782980, 1001002, 2, date(2023, 4, 26), "11:37:41"),
#     ]
#     df_columns = ["hierarchy_n1", "badge", "device", "ticket_number", "date", "hour"]
#     df = spark_session.createDataFrame(data, df_columns)

#     date_data = [
#         (date(2023, 4, 19), 2023, "P4", "W4"),
#         (date(2023, 4, 24), 2023, "P5", "W1"),
#         (date(2023, 4, 26), 2023, "P5", "W1"),
#     ]
#     date_df_columns = ["event_date", "year", "periods", "week"]
#     date_df = spark_session.createDataFrame(date_data, date_df_columns)

#     df_result = rh.calculate_mean_meals_per_weekday(df, date_df)

#     expected_data = [
#         (date(2023, 4, 19), 2023, "P4", "W4", "Wednesday", 3.5),
#         (date(2023, 4, 24), 2023, "P5", "W1", "Monday", 2.0),
#         (date(2023, 4, 26), 2023, "P5", "W1", "Wednesday", 3.5),
#     ]
#     expected_columns = [
#         "date",
#         "year",
#         "periods",
#         "week",
#         "jour_semaine",
#         "moyenne_repas",
#     ]
#     expected_df = spark_session.createDataFrame(expected_data, expected_columns)

#     assert df_result.collect() == expected_df.collect()


# def test_calculate_interval_table(spark_session):
#     data = [
#         ("Usine Aimargues", 739847703300, 1001002, 1, date(2023, 4, 19), "11:33:31"),
#         ("Royal Canin Siege", 411414782980, 1001002, 2, date(2023, 4, 19), "11:37:41"),
#         ("Royal Canin Siege", 983049251588, 1001002, 27, date(2023, 4, 19), "12:07:18"),
#         ("Royal Canin Siege", 983049251588, 1001002, 28, date(2023, 4, 19), "12:07:18"),
#         ("Usine Aimargues", 739847703300, 1001002, 1, date(2023, 4, 24), "11:33:31"),
#         ("Royal Canin Siege", 411414782980, 1001002, 2, date(2023, 4, 24), "11:37:41"),
#         ("Royal Canin Siege", 983049251588, 1001002, 27, date(2023, 4, 26), "12:07:18"),
#         ("Usine Aimargues", 739847703300, 1001002, 1, date(2023, 4, 26), "12:33:31"),
#         ("Royal Canin Siege", 411414782980, 1001002, 2, date(2023, 4, 26), "12:37:41"),
#     ]
#     df_columns = ["hierarchy_n1", "badge", "device", "ticket_number", "date", "hour"]
#     df = spark_session.createDataFrame(data, df_columns)

#     canteen_data = [
#         (date(2023, 4, 19), 2023, "P4", "W4", "Wednesday", 3.5),
#         (date(2023, 4, 24), 2023, "P5", "W1", "Monday", 2.0),
#         (date(2023, 4, 26), 2023, "P5", "W1", "Wednesday", 3.5),
#     ]
#     canteen_columns = [
#         "date",
#         "year",
#         "periods",
#         "week",
#         "jour_semaine",
#         "moyenne_repas",
#     ]
#     canteen_df = spark_session.createDataFrame(canteen_data, canteen_columns)

#     value_start = 10
#     value_interval = 15

#     df_result = rh.calculate_interval_table(df, canteen_df, value_start, value_interval)

#     expected_data = [
#         (date(2023, 4, 19), 2023, "P4", "W4", "Wednesday", 3.5, "11:30:00", 2),
#         (date(2023, 4, 19), 2023, "P4", "W4", "Wednesday", 3.5, "12:00:00", 2),
#         (date(2023, 4, 24), 2023, "P5", "W1", "Monday", 2.0, "11:30:00", 2),
#         (date(2023, 4, 26), 2023, "P5", "W1", "Wednesday", 3.5, "12:00:00", 1),
#         (date(2023, 4, 26), 2023, "P5", "W1", "Wednesday", 3.5, "12:30:00", 2),
#     ]
#     expected_columns = [
#         "date",
#         "year",
#         "periods",
#         "week",
#         "jour_semaine",
#         "moyenne_repas",
#         "interval_hour",
#         "number_of_meals",
#     ]
#     expected_df = spark_session.createDataFrame(expected_data, expected_columns)

#     assert df_result.collect() == expected_df.collect()
