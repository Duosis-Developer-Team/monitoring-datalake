"""Unit tests for the Normalizer, exercising the mapping table directly."""

from __future__ import annotations

from app.sources.obm_agent import MAPPING_PATH
from app.sources.obm_agent.normalization import Normalizer, NormalizationConfig


def _normalizer() -> Normalizer:
    return Normalizer(NormalizationConfig.from_path(MAPPING_PATH))


def test_gbl_cpu_total_util_maps_to_cpu_util_pct():
    n = _normalizer()
    records = n.normalize(
        {"class_name": "GLOBAL", "metrics": {"GBL_CPU_TOTAL_UTIL": 42.3}},
        request_id="t1",
    )
    assert records[0]["metrics"]["cpu_util_pct"] == 42.3


def test_gbl_cpu_user_mode_util_maps():
    n = _normalizer()
    records = n.normalize(
        {"class_name": "GLOBAL", "metrics": {"GBL_CPU_USER_MODE_UTIL": 12.1}},
        request_id="t2",
    )
    assert records[0]["metrics"]["cpu_user_mode_util_pct"] == 12.1


def test_gbl_mem_util_maps():
    n = _normalizer()
    records = n.normalize(
        {"class_name": "GLOBAL", "metrics": {"GBL_MEM_UTIL": 68.4}},
        request_id="t3",
    )
    assert records[0]["metrics"]["mem_util_pct"] == 68.4


def test_uptime_seconds_maps():
    n = _normalizer()
    records = n.normalize(
        {"class_name": "GLOBAL", "metrics": {"GBL_SYSTEM_UPTIME_SECONDS": 86400}},
        request_id="t4",
    )
    assert records[0]["metrics"]["uptime_s"] == 86400


def test_bydsk_devname_maps_to_disk_device_name():
    n = _normalizer()
    records = n.normalize(
        {"class_name": "DISK", "metrics": {"BYDSK_DEVNAME": "sda", "BYDSK_UTIL": 80.0}},
        request_id="t5",
    )
    assert records[0]["target_table"] == "opsb_agent_disk"
    assert records[0]["metrics"]["disk_device_name"] == "sda"
    assert records[0]["metrics"]["disk_util_pct"] == 80.0


def test_fs_space_util_maps():
    n = _normalizer()
    records = n.normalize(
        {"class_name": "FILESYSTEM", "metrics": {"FS_SPACE_UTIL": 55.0}},
        request_id="t6",
    )
    assert records[0]["target_table"] == "opsb_agent_filesys"
    assert records[0]["metrics"]["fs_util_pct"] == 55.0


def test_bycpu_id_maps():
    n = _normalizer()
    records = n.normalize(
        {"class_name": "CPU", "metrics": {"BYCPU_ID": "cpu0"}},
        request_id="t7",
    )
    assert records[0]["target_table"] == "opsb_agent_cpu"
    assert records[0]["metrics"]["cpu_id"] == "cpu0"


def test_bynetif_name_and_util_map():
    n = _normalizer()
    records = n.normalize(
        {
            "class_name": "NETIF",
            "metrics": {"BYNETIF_NAME": "eth0", "BYNETIF_UTIL": 30.0},
        },
        request_id="t8",
    )
    assert records[0]["target_table"] == "opsb_agent_netif"
    assert records[0]["metrics"]["netif_name"] == "eth0"
    assert records[0]["metrics"]["netif_util_pct"] == 30.0


def test_unknown_class_does_not_crash():
    n = _normalizer()
    records = n.normalize(
        {"class_name": "TOTALLY_NEW_CLASS", "metrics": {"BRAND_NEW_METRIC": 1.0}},
        request_id="t9",
    )
    assert records[0]["class_name"] == "TOTALLY_NEW_CLASS"
    assert records[0]["target_table"] is None
    assert records[0]["extra_metrics"]["BRAND_NEW_METRIC"] == 1.0


def test_class_inferred_from_prefixes_when_no_class_name():
    n = _normalizer()
    records = n.normalize(
        {"metrics": {"GBL_CPU_TOTAL_UTIL": 42.3, "GBL_MEM_UTIL": 60.0}},
        request_id="t10",
    )
    assert records[0]["class_name"] == "GLOBAL"
    assert records[0]["metrics"]["cpu_util_pct"] == 42.3


def test_class_unknown_when_no_inference_possible():
    n = _normalizer()
    records = n.normalize(
        {"metrics": {"FOO": 1, "BAR": 2}},
        request_id="t11",
    )
    assert records[0]["class_name"] == "UNKNOWN"


def test_metric_pairs_inferred_from_top_level_when_no_metrics_wrapper():
    n = _normalizer()
    records = n.normalize(
        {"class_name": "GLOBAL", "GBL_CPU_TOTAL_UTIL": 42.3, "MonitoredSystem": "x"},
        request_id="t12",
    )
    assert records[0]["metrics"]["cpu_util_pct"] == 42.3
    assert records[0]["common"]["node_fqdn"] == "x"


def test_common_fields_extracted():
    n = _normalizer()
    records = n.normalize(
        {
            "CollectionConfigName": "OOTB_AgentMetricCollection",
            "MonitoredSystem": "host.example.local",
            "MonitoredSystemID": "host",
            "tenant_id": "example-tenant",
            "timestamp_utc": 1779638400,
            "class_name": "GLOBAL",
            "metrics": {"GBL_CPU_TOTAL_UTIL": 10.0},
        },
        request_id="t13",
    )
    common = records[0]["common"]
    assert common["collection_policy_name"] == "OOTB_AgentMetricCollection"
    assert common["node_fqdn"] == "host.example.local"
    assert common["node_short_name"] == "host"
    assert common["tenant_id"] == "example-tenant"
    assert common["timestamp_utc_s"] == 1779638400


def test_records_wrapper_extracted():
    n = _normalizer()
    records = n.normalize(
        {
            "records": [
                {"class_name": "GLOBAL", "metrics": {"GBL_CPU_TOTAL_UTIL": 1.0}},
                {"class_name": "DISK", "metrics": {"BYDSK_UTIL": 2.0}},
            ]
        },
        request_id="t14",
    )
    assert len(records) == 2
    assert records[0]["class_name"] == "GLOBAL"
    assert records[1]["class_name"] == "DISK"
