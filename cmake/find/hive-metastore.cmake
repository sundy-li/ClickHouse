set (HIVE_METASTORE_INCLUDE_DIR "${ClickHouse_SOURCE_DIR}/contrib/hive-metastore/src")
list(APPEND HIVE_METASTORE_INCLUDE_DIR "${ClickHouse_SOURCE_DIR}/contrib/hive-metastore/if/gen-cpp")

set (HIVE_METASTORE_LIBRARY "hive-metastore")