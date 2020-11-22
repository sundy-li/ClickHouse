#pragma once

#include "config_core.h"

#include <mysqlxx/Pool.h>

#include <Core/MultiEnum.h>
#include <Common/ThreadPool.h>
#include <Databases/DatabasesCommon.h>
#include <Databases/MySQL/ConnectionMySQLSettings.h>
#include <Parsers/ASTCreateQuery.h>

#include <atomic>
#include <condition_variable>
#include <map>
#include <memory>
#include <mutex>
#include <unordered_set>
#include <vector>

namespace DB
{

class Context;

/** Real-time access to table list and table structure from remote
 *  It doesn't make any manipulations with filesystem.
 *  All tables are created by calling code after real-time pull-out structure from remote
 */
template <typename Derived>
class DatabaseRemote : public IDatabase
{
public:
    ~DatabaseRemote() override;

    DatabaseRemote(
        const Context & context, const String & database_name, const String & metadata_path,
        const ASTStorage * database_engine_define, const String & database_name_in_mysql, std::unique_ptr<ConnectionMySQLSettings> settings_,
        mysqlxx::Pool && pool);

    Derived & derived() { return static_cast<Derived &>(*this); }

    String getEngineName() const override { return derived.getEngineName(); }

    bool canContainMergeTreeTables() const override { return false; }

    bool canContainDistributedTables() const override { return false; }

    bool shouldBeEmptyOnDetach() const override { return false; }

    bool empty() const override;

    DatabaseTablesIteratorPtr getTablesIterator(const Context & context, const FilterByNameFunction & filter_by_table_name) override;

    ASTPtr getCreateDatabaseQuery() const override;

    bool isTableExist(const String & name, const Context & context) const override;

    StoragePtr tryGetTable(const String & name, const Context & context) const override;

    time_t getObjectMetadataModificationTime(const String & name) const override;

    void shutdown() override;

    void drop(const Context & /*context*/) override;

    String getMetadataPath() const override;

    void createTable(const Context &, const String & table_name, const StoragePtr & storage, const ASTPtr & create_query) override;

    void loadStoredObjects(Context &, bool, bool force_attach) override;

    StoragePtr detachTable(const String & table_name) override;

    void dropTable(const Context &, const String & table_name, bool no_delay) override;

    void attachTable(const String & table_name, const StoragePtr & storage, const String & relative_table_path) override;

protected:
    ASTPtr getCreateTableQueryImpl(const String & name, const Context & context, bool throw_on_error) const override;

private:
    const Context & global_context;
    String metadata_path;
    ASTPtr database_engine_define;
};

}
