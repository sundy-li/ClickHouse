#pragma once

#include <Common/config.h>

#if USE_HDFS
#    include <DataStreams/IBlockOutputStream.h>
#    include <IO/HDFSCommon.h>
#    include <Common/ThreadPool.h>

namespace DB
{
class StorageHive;
struct StorageInMemoryMetadata;
using StorageMetadataPtr = std::shared_ptr<const StorageInMemoryMetadata>;

class HiveBlockOutputStream : public IBlockOutputStream
{
public:
    HiveBlockOutputStream(
        StorageHive & storage_,
        const StorageMetadataPtr & metadata_snapshot_,
        const Context & context_,
        size_t max_parts_per_block_,
        size_t concurrency_ = 1);

    Block getHeader() const override;
    void writePrefix() override;
    void write(const Block & block) override;
    void writeSuffix() override;

private:
    void sendBlock(const Block & block_maybe_const);

    StorageHive & storage;
    StorageMetadataPtr metadata_snapshot;
    const Context & context;
    size_t max_parts_per_block;
    ThreadPool pool;
    size_t concurrency;
    String name_node_url;
    String location_path;
    String format;
    uint64_t thread_id;
    bool init = false;
    std::atomic<size_t> cur = 0;
};
}

#endif
