#pragma once

#include <Common/config.h>

#if USE_HDFS
#include <DataStreams/IBlockOutputStream.h>



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
        const size_t & max_parts_per_block_);

    Block getHeader() const override;
    void write(const Block & block) override;

private:
    StorageHive & storage;
    StorageMetadataPtr metadata_snapshot;
    const Context & context;
    size_t max_parts_per_block;
};
}

#endif
