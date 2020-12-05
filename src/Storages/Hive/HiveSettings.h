#pragma once

#include <Core/BaseSettings.h>
#include <Core/Settings.h>


namespace DB
{
class ASTStorage;


#define HIVE_RELATED_SETTINGS(M) \
    M(String, hdfs_namenode, "", "HDFS namenode url(can be hdfs router url, viewfs or any service compatible with hdfs rpc protocol), this will replace host of hdfs url if not empty.", 0)
#define LIST_OF_HIVE_SETTINGS(M) \
    HIVE_RELATED_SETTINGS(M) \
    FORMAT_FACTORY_SETTINGS(M)

DECLARE_SETTINGS_TRAITS(HiveSettingsTraits, LIST_OF_HIVE_SETTINGS)


/** Settings for the Hive engine.
  * Could be loaded from a CREATE TABLE query (SETTINGS clause).
  */
struct HiveSettings : public BaseSettings<HiveSettingsTraits>
{
    void loadFromQuery(ASTStorage & storage_def);
};

}
