#pragma once
#if !defined(ARCADIA_BUILD)
#    include "config_core.h"
#endif

#if USE_HDFS
#    include <DataTypes/IDataType.h>
#    include <TableFunctions/ITableFunction.h>
#    include <metastore/HiveMetastoreCommon.h>

#    include <unordered_map>


namespace Apache::Hadoop::Hive
{
class Table;
}

namespace DB
{
/* hive ('metastore_uri', 'hive_database', 'hive_table', 'hdfs_uri') - creates a temporary StorageHive.
 * The structure of the table is taken from the mysql query DESCRIBE table.
 * If there is no such table, an exception is thrown.
 */
class TableFunctionHive : public ITableFunction
{
public:
    static constexpr auto name = "hive";
    static std::unordered_map<String, DataTypePtr> type_mappers;

    std::string getName() const override { return name; }

private:
    StoragePtr
    executeImpl(const ASTPtr & ast_function, const Context & context, const std::string & table_name, ColumnsDescription cached_columns)
        const override;
    const char * getStorageTypeName() const override { return "Hive"; }

    ColumnsDescription getActualTableStructure(const Context & context) const override;
    void parseArguments(const ASTPtr & ast_function, const Context & context) override;

    DataTypePtr convertHiveDataType(const String & type_name, bool is_null) const;

    String metastore_uri;
    String hive_database;
    String hive_table;
    String hdfs_uri;

    mutable ASTPtr partition_by_ast;
    mutable Apache::Hadoop::Hive::Table table;
};

DataTypePtr convertHiveDataType(const String & type_name, bool is_null);

}

#endif
