#pragma once
#include <Common/config.h>
#if USE_HDFS

#include <Storages/IStorage.h>
#include <Poco/URI.h>
#include <IO/ConnectionTimeouts.h>
#include <common/logger_useful.h>
#include <ext/shared_ptr_helper.h>

namespace DB
{
/**
 * This class represents table engine for external hive store.
 * Read method is supported for now.
 */
class StorageHive final : public ext::shared_ptr_helper<StorageHive>, public IStorage
{
    friend struct ext::shared_ptr_helper<StorageHive>;
public:
    String getName() const override { return "Hive"; }

    Pipe read(
        const Names & column_names,
        const StorageMetadataPtr & /*metadata_snapshot*/,
        SelectQueryInfo & query_info,
        const Context & context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        unsigned num_streams) override;

    NamesAndTypesList getVirtuals() const override;

protected:
    StorageHive(const String & metastore_url_,
        const String & hive_database_,
        const String & hive_table_,
        const StorageID & table_id_,
        const ColumnsDescription & columns_,
        const ConstraintsDescription & constraints_,
        const ASTPtr & partition_by_ast_,
        Context & context_);

private:
    String metastore_url;
    String hive_database;
    String hive_table;
    String format_name;
    const ASTPtr partition_by_ast;
    Context & context;
    ConnectionTimeouts timeouts;
    String compression_method;

    NamesAndTypesList partition_name_types;
    ExpressionActionsPtr minmax_idx_expr;

    Poco::Logger * log = &Poco::Logger::get("StorageHive");
};
}

#endif
