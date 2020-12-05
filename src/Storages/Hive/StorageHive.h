#pragma once
#include <Common/config.h>
#if USE_HDFS

#include <Storages/IStorage.h>
#include <Storages/Hive/HiveSettings.h>
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
    friend class HiveBlockOutputStream;
    friend class HiveBlockInputStream;

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

    bool supportsParallelInsert() const override { return true; }
    BlockOutputStreamPtr write(const ASTPtr & /*query*/, const StorageMetadataPtr & metadata_snapshot, const Context & /*context_*/) override;
    NamesAndTypesList getVirtuals() const override;

protected:
    StorageHive(const StorageID & table_id_,
        const String & metastore_url_,
        const String & hive_database_,
        const String & hive_table_,
        const ColumnsDescription & columns_,
        std::unique_ptr<HiveSettings> hive_settings_,
        const ConstraintsDescription & constraints_,
        const ASTPtr & partition_by_ast_,
        const Context & context_);

private:
    String metastore_url;
    String hive_database;
    String hive_table;
    std::unique_ptr<HiveSettings> hive_settings;
    const ASTPtr partition_by_ast;
    const Context & context;
    ConnectionTimeouts timeouts;

    NamesAndTypesList partition_name_types;
    ExpressionActionsPtr minmax_idx_expr;

    Poco::Logger * log = &Poco::Logger::get("StorageHive");
};

String convertHiveFormat(const String & hive_format);

}

#endif
