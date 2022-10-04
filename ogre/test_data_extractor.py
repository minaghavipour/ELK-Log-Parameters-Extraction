import pytest

from ogre.data_extractor import DateRange, ElasticDataExtractor


@pytest.fixture()
def elastic_data_extractor():
    extractor = ElasticDataExtractor("http://185.112.150.48:9200/", "hayoola")
    return extractor


def test_extracted_main_log_data(elastic_data_extractor):
    dataframes = elastic_data_extractor.get_extracted_features_from_main_log(
        DateRange("2022-07-04T09:40:00.000+04:30", "2022-07-04T13:41:00.000+04:30"),
    )

    assert dataframes["m1-r1z1s48"].loc[0]["doc_count"] == 161
    assert (
        dataframes["m1-r1z1s48"].loc[0]["key_as_string"] == "2022-07-04T05:10:00.000Z"
    )
    assert dataframes["m1-r1z1s48"].loc[0]["stats-of-bytes-recvd.sum"] == 949661.0
    assert dataframes["m1-r1z1s48"].loc[0]["stats-of-bytes-sent.sum"] == 0.0
    assert dataframes["m1-r1z1s48"].loc[0]["container-server.count"] == 10.0
    assert dataframes["m1-r1z1s48"].loc[0]["proxy-server.count"] == 79.0
    assert dataframes["m1-r1z1s48"].loc[0]["account-server.count"] == 70.0
    assert dataframes["m1-r1z1s48"].loc[0]["object-server.count"] == 2.0
    assert dataframes["m1-r1z1s48"].loc[0]["PUT.count"] == 73.0
    assert dataframes["m1-r1z1s48"].loc[0]["GET.count"] == 1.0
    assert (
        round(
            dataframes["m1-r1z1s48"].loc[0]["stats-of-request-time-put.avg"],
            3,
        )
        == 0.027
    )
    assert (
        round(
            dataframes["m1-r1z1s48"].loc[0]["stats-of-request-time-get.avg"],
            3,
        )
        == 0.005
    )
    assert dataframes["m1-r1z1s48"].loc[0]["stats-of-request-time-delete.avg"] == 0
    assert dataframes["m1-r1z1s48"].loc[0]["stats-of-request-time-options.avg"] == 0
    assert dataframes["m1-r1z1s48"].loc[0]["stats-of-request-time-post.avg"] == 0
    assert dataframes["m1-r1z1s48"].loc[0]["status-int-2XX.count"] == 161.0
    assert dataframes["m1-r1z1s48"].loc[0]["status-int-3XX.count"] == 0
    assert dataframes["m1-r1z1s48"].loc[0]["status-int-4XX.count"] == 0
    assert dataframes["m1-r1z1s48"].loc[0]["status-int-5XX.count"] == 0


def test_extracted_all_log_data(elastic_data_extractor):
    dataframes = elastic_data_extractor.get_extracted_features_from_all_log(
        DateRange("2022-07-04T09:40:00.000+04:30", "2022-07-04T13:41:00.000+04:30"),
    )

    assert dataframes["m1-r1z1s48"].loc[0]["doc_count"] == 191
    assert dataframes["m1-r1z1s48"].loc[0]["container-server.count"] == 10.0
    assert dataframes["m1-r1z1s48"].loc[0]["proxy-server.count"] == 79.0
    assert dataframes["m1-r1z1s48"].loc[0]["account-server.count"] == 70.0
    assert dataframes["m1-r1z1s48"].loc[0]["object-server.count"] == 2.0
    assert dataframes["m1-r1z1s48"].loc[0]["container-auditor.count"] == 0
    assert dataframes["m1-r1z1s48"].loc[0]["object-auditor.count"] == 0
    assert dataframes["m1-r1z1s48"].loc[0]["container-replicator.count"] == 6
    assert dataframes["m1-r1z1s48"].loc[0]["object-updater.count"] == 0
    assert dataframes["m1-r1z1s48"].loc[0]["object-replicator.count"] == 6
    assert dataframes["m1-r1z1s48"].loc[0]["container-sync.count"] == 0
    assert dataframes["m1-r1z1s48"].loc[0]["sum-of-replication-fail.value"] == 0.0
    assert dataframes["m1-r1z1s48"].loc[0]["sum-of-replication-success.value"] == 23.0
    assert (
        round(
            dataframes["m1-r1z1s48"].loc[0][
                "sum-of-total-replicator-time-minutes.value"
            ],
            3,
        )
        == 0.070
    )
