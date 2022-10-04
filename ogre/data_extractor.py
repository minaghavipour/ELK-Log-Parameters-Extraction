from typing import Dict, List, Optional
from dataclasses import dataclass

import pandas as pd
from elasticsearch import Elasticsearch
from elasticsearch_dsl import Q, Search
from elasticsearch_dsl.aggs import Sum, Terms, Filter, ExtendedStats
from elasticsearch_dsl.response import Response


@dataclass
class DateRange:
    start_date: str
    end_date: str


class ElasticDataExtractor:
    HTTP_VERBS = ["PUT", "GET", "OPTIONS", "POST", "DELETE"]
    STATS = ["sum", "avg", "min", "max", "std_deviation"]
    SERVICES = [
        "container-server",
        "object-server",
        "proxy-server",
        "account-server",
        "container-auditor",
        "object-auditor",
        "container-replicator",
        "object-updater",
        "object-replicator",
        "container-sync",
        "account-auditor",
        "container-updater",
        "account-replicator",
    ]

    def __init__(
        self,
        connection_url: str,
        index: str,
        user: Optional[str] = None,
        password: Optional[str] = None,
    ):
        self.user = user
        self.password = password
        self.connection_url = connection_url
        self.index = index
        self.elastic_client = Elasticsearch(hosts=self.connection_url)

    @staticmethod
    def _generate_request_time_query(http_verbs: List[str]) -> Dict[str, Filter]:
        stats_of_request_time_verb_aggs = {}
        for verb in http_verbs:
            stats_of_request_time_verb_agg = Filter(
                Q(
                    "bool",
                    filter=[
                        {
                            "match_phrase": {
                                "programname": "proxy-server",
                            },
                        },
                        {
                            "match_phrase": {
                                "request_method": verb,
                            },
                        },
                    ],
                    must_not={
                        "match": {
                            "user_agent": "Swift",
                        },
                    },
                ),
            )

            stats_of_request_time_verb_agg.bucket(
                "req-time",
                ExtendedStats(field="request_time"),
            )
            stats_of_request_time_verb_aggs.update(
                {verb: stats_of_request_time_verb_agg},
            )

        return stats_of_request_time_verb_aggs

    @staticmethod
    def _post_process(df: pd.DataFrame) -> pd.DataFrame:
        status_code_2xx = pd.Series([0 for _ in range(len(df))])
        for column in df.columns[df.columns.str.startswith("20")]:
            status_code_2xx += df[column]
        df["status-int-2XX.count"] = status_code_2xx

        status_code_3xx = pd.Series([0 for _ in range(len(df))])
        for column in df.columns[df.columns.str.startswith("30")]:
            status_code_3xx += df[column]
        df["status-int-3XX.count"] = status_code_3xx

        status_code_4xx = pd.Series([0 for _ in range(len(df))])
        for column in df.columns[df.columns.str.startswith("40")]:
            status_code_4xx += df[column]
        df["status-int-4XX.count"] = status_code_4xx

        status_code_5xx = pd.Series([0 for _ in range(len(df))])
        for column in df.columns[df.columns.str.startswith("50")]:
            status_code_5xx += df[column]
        df["status-int-5XX.count"] = status_code_5xx

        processed_df = ElasticDataExtractor._select_some_columns(df)

        processed_df.fillna(0, inplace=True)

        processed_df = ElasticDataExtractor._rename_some_columns(processed_df)

        return processed_df

    @staticmethod
    def _convert_to_df(
        response: Response,
        bucketized_feature_names: list[str],
    ) -> pd.DataFrame:
        df = pd.json_normalize(
            response["sliding_windows"]["buckets"],
        )

        bucketized_feature_dataframes = [df]
        for bucketized_feature_name in bucketized_feature_names:
            bucketized_feature_series = pd.Series(
                [
                    {x["key"]: x["doc_count"] for x in row}
                    for row in df[f"{bucketized_feature_name}.buckets"].values
                ],
            )
            bucketized_feature_dataframe = pd.DataFrame(
                bucketized_feature_series.tolist(),
            )
            bucketized_feature_dataframes.append(bucketized_feature_dataframe)

        df = pd.concat(bucketized_feature_dataframes, axis="columns")

        df = ElasticDataExtractor._post_process(df)
        return df

    @classmethod
    def _select_some_columns(cls, df: pd.DataFrame) -> pd.DataFrame:
        selected_columns = [
            "key_as_string",
            "key",
            "doc_count",
            "sum-of-replication-success.value",
            "sum-of-replication-fail.value",
            "sum-of-total-replicator-time-minutes.value",
            "status-int-2XX.count",
            "status-int-3XX.count",
            "status-int-4XX.count",
            "status-int-5XX.count",
        ]

        for stat in cls.STATS:
            selected_columns.append(f"stats-of-bytes-recvd.bytes-recvd-stats.{stat}")

        for stat in cls.STATS:
            selected_columns.append(f"stats-of-bytes-sent.bytes-sent-stats.{stat}")

        for verb in cls.HTTP_VERBS:
            for stat in cls.STATS:
                selected_columns.append(
                    f"stats-of-request-time-{verb.lower()}.req-time.{stat}",
                )

        for service in cls.SERVICES:
            selected_columns.append(service)

        for verb in cls.HTTP_VERBS:
            selected_columns.append(verb)

        for not_exist_column in set(selected_columns).difference(set(df.columns)):
            df[not_exist_column] = 0

        processed_df = df[selected_columns].copy()
        return processed_df

    @classmethod
    def _rename_some_columns(cls, df: pd.DataFrame) -> pd.DataFrame:
        name_mapping = {}

        for service in cls.SERVICES:
            name_mapping.update(
                {
                    service: f"{service}.count",
                },
            )

        for stat in cls.STATS:
            name_mapping.update(
                {
                    f"stats-of-bytes-recvd.bytes-recvd-stats.{stat}": f"stats-of-bytes-recvd.{stat}",
                },
            )

        for stat in cls.STATS:
            name_mapping.update(
                {
                    f"stats-of-bytes-sent.bytes-sent-stats.{stat}": f"stats-of-bytes-sent.{stat}",
                },
            )

        for verb in cls.HTTP_VERBS:
            for stat in cls.STATS:
                name_mapping.update(
                    {
                        f"stats-of-request-time-{verb.lower()}.req-time.{stat}": f"stats-of-request-time-{verb.lower()}.{stat}",
                    },
                )

        for verb in cls.HTTP_VERBS:
            name_mapping.update(
                {
                    verb: f"{verb}.count",
                },
            )

        processed_df = df.rename(columns=name_mapping)
        return processed_df

    @classmethod
    def _extract_features(cls, search_query: Search) -> Dict[str, pd.DataFrame]:
        stats_of_bytes_received_agg = Filter(
            Q(
                "bool",
                filter={
                    "match_phrase": {
                        "request_method": "PUT",
                    },
                },
            ),
        )
        stats_of_bytes_received_agg.bucket(
            "bytes-recvd-stats",
            ExtendedStats(field="bytes_recvd"),
        )
        stats_of_bytes_sent_agg = Filter(
            Q(
                "bool",
                filter={
                    "match_phrase": {
                        "request_method": "GET",
                    },
                },
            ),
        )
        stats_of_bytes_sent_agg.bucket(
            "bytes-sent-stats",
            ExtendedStats(field="bytes_sent"),
        )
        value_counts_of_programname_agg = Terms(field="programname.keyword")
        value_counts_of_severity_agg = Terms(field="severity.keyword")
        value_counts_of_request_method_agg = Filter(
            Q(
                "bool",
                filter={
                    "match_phrase": {
                        "programname": "proxy-server",
                    },
                },
                must_not={
                    "match": {
                        "user_agent": "Swift",
                    },
                },
            ),
        )
        value_counts_of_request_method_agg.bucket(
            "req-count",
            Terms(field="request_method.keyword"),
        )
        value_counts_of_status_int_agg = Terms(field="status_int.keyword")
        stats_of_request_time_aggs = ElasticDataExtractor._generate_request_time_query(
            cls.HTTP_VERBS,
        )
        sum_of_replication_fail_agg = Sum(field="ReplicationFail")
        sum_of_replication_success_agg = Sum(field="ReplicationSuccess")
        sum_of_total_replicator_time_agg = Sum(field="TotalReplicatorTimeMinutes")

        search_query.aggs.bucket("node", "terms", field="sysloghost.keyword").bucket(
            "sliding_windows",
            "date_histogram",
            field="@timestamp",
            interval="30s",
        ).metric("stats-of-bytes-recvd", stats_of_bytes_received_agg).metric(
            "stats-of-bytes-sent",
            stats_of_bytes_sent_agg,
        ).metric(
            "value-counts-of-programname",
            value_counts_of_programname_agg,
        ).metric(
            "value-counts-of-severity",
            value_counts_of_severity_agg,
        ).metric(
            "value-counts-of-request-method",
            value_counts_of_request_method_agg,
        ).metric(
            "sum-of-replication-fail",
            sum_of_replication_fail_agg,
        ).metric(
            "sum-of-replication-success",
            sum_of_replication_success_agg,
        ).metric(
            "sum-of-total-replicator-time-minutes",
            sum_of_total_replicator_time_agg,
        ).metric(
            "value-counts-of-status-int",
            value_counts_of_status_int_agg,
        )

        for (
            verb,
            stats_of_req_time_agg,
        ) in stats_of_request_time_aggs.items():
            search_query.aggs["node"]["sliding_windows"].metric(
                "stats-of-request-time-" + verb.lower(),
                stats_of_req_time_agg,
            )

        response = search_query.execute()

        dict_of_dataframes = {}
        for node in response.aggregations.to_dict()["node"]["buckets"]:
            df = ElasticDataExtractor._convert_to_df(
                response=node,
                bucketized_feature_names=[
                    "value-counts-of-programname",
                    "value-counts-of-severity",
                    "value-counts-of-request-method.req-count",
                    "value-counts-of-status-int",
                ],
            )
            dict_of_dataframes.update({node["key"]: df})

        return dict_of_dataframes

    def get_extracted_features_from_main_log(
        self,
        date_range: DateRange,
    ) -> Dict[str, pd.DataFrame]:
        # query: The elasticsearch query.
        search_query = Search(using=self.elastic_client)

        search_query = search_query.query(
            "bool",
            filter=[
                {
                    "range": {
                        "@timestamp": {
                            "gte": date_range.start_date,
                            "lte": date_range.end_date,
                        },
                    },
                },
                {
                    "match": {
                        "severity": "info",
                    },
                },
                {
                    "bool": {
                        "should": [
                            {
                                "match_phrase": {
                                    "programname": "object-server",
                                },
                            },
                            {
                                "match_phrase": {
                                    "programname": "container-server",
                                },
                            },
                            {
                                "match_phrase": {
                                    "programname": "account-server",
                                },
                            },
                            {
                                "match_phrase": {
                                    "programname": "proxy-server",
                                },
                            },
                        ],
                        "minimum_should_match": 1,
                    },
                },
            ],
        )

        dict_of_df_per_node = self._extract_features(search_query)

        return dict_of_df_per_node

    def get_extracted_features_from_all_log(
        self,
        date_range: DateRange,
    ) -> Dict[str, pd.DataFrame]:
        # query: The elasticsearch query.
        search_query = Search(using=self.elastic_client)

        search_query = search_query.query(
            "bool",
            filter=[
                {
                    "range": {
                        "@timestamp": {
                            "gte": date_range.start_date,
                            "lte": date_range.end_date,
                        },
                    },
                },
                {
                    "match": {
                        "severity": "info",
                    },
                },
            ],
        )

        dict_of_df_per_node = self._extract_features(search_query)

        return dict_of_df_per_node
