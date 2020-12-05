#include <Common/config.h>

#if USE_HDFS

#include <Storages/Hive/HiveBlockOutputStream.h>
#include <Storages/Hive/StorageHive.h>
#include <Storages/MergeTree/MergeTreeDataWriter.h>
#include <Formats/FormatFactory.h>
#include <IO/WriteBufferFromString.h>
#include <IO/CompressionMethod.h>
#include <IO/HDFSCommon.h>
#include <IO/WriteBufferFromHDFS.h>
#include <Common/StringUtils/StringUtils.h>
#include <Common/escapeForFileName.h>

#include <common/logger_useful.h>
#include <common/getFQDNOrHostName.h>

#include <Poco/URI.h>
#include <hdfs/hdfs.h>
#include <boost/algorithm/string.hpp>

#include <metastore/HiveMetastoreCommon.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int INVALID_PARTITION_VALUE;
    extern const int CANNOT_CREATE_DIRECTORY;
}

HiveBlockOutputStream::HiveBlockOutputStream(
    StorageHive & storage_,
    const StorageMetadataPtr & metadata_snapshot_,
    const Context & context_,
    const size_t & max_parts_per_block_)
    : storage(storage_)
    , metadata_snapshot(metadata_snapshot_)
    , context(context_)
    , max_parts_per_block(max_parts_per_block_)
{
}

Block HiveBlockOutputStream::getHeader() const
{
    return metadata_snapshot->getSampleBlock();
}

void HiveBlockOutputStream::write(const Block & block)
{
    metadata_snapshot->check(block, true);
    Poco::URI metastore_uri(storage.metastore_url);
    HMSManager hsm_manager(metastore_uri.getHost(), metastore_uri.getPort());

    auto & client = hsm_manager.getClient();
    Apache::Hadoop::Hive::Table table;
    client.get_table(table, storage.hive_database, storage.hive_table);

    size_t begin_of_path = table.sd.location.find('/', table.sd.location.find("//") + 2);
    String uri_without_path = table.sd.location.substr(0, begin_of_path) + "/";
    String location_path = table.sd.location.substr(begin_of_path);

    String name_node_url = uri_without_path;
    if (!storage.hive_settings->hdfs_namenode.value.empty())
    {
        name_node_url = storage.hive_settings->hdfs_namenode.value;
        boost::algorithm::trim_right_if(name_node_url, [](const char & c) { return c == '/'; });
        name_node_url += "/";
    }

    HDFSBuilderPtr builder = createHDFSBuilder(name_node_url);
    HDFSFSPtr fs = createHDFSFS(builder.get());

    //split the block by partition keys
    auto part_blocks = MergeTreeDataWriter::splitBlockIntoParts(block, max_parts_per_block, metadata_snapshot);
    auto names = storage.partition_name_types.getNames();
    for (size_t idx = 0; idx < part_blocks.size(); ++idx)
    {
        auto & part_block = part_blocks[idx];
        String location_dir = location_path;
        boost::algorithm::trim_right_if(location_dir, [](const char & c) { return c == '/'; });

        if (names.size() != part_block.partition.size())
            throw Exception("Partition size not matched.", ErrorCodes::INVALID_PARTITION_VALUE);

        for (size_t i = 0; i < names.size(); ++i)
        {
            location_dir += "/" + names[i] + "=" + toString(part_block.partition[i]);
            part_block.block.erase(names[i]);
        }

        auto info = hdfsGetPathInfo(fs.get(), location_dir.c_str());
        auto partition_exists = info && info->mKind == 'D';

        if (!partition_exists)
        {
            auto res = hdfsCreateDirectory(fs.get(), location_dir.c_str());
            if (res < 0)
                throw Exception("Can't create hdfs directory: " + location_dir + ": ", ErrorCodes::CANNOT_CREATE_DIRECTORY);
        }

        String file = fmt::format("{}/{}_{}_{}_{}",
                                location_dir,
                                escapeForFileName(getFQDNOrHostName()),
                                context.getTCPPort(),
                                context.getClientInfo().current_query_id,
                                idx);

        auto write_buf = std::make_unique<WriteBufferFromHDFS>(name_node_url + file, storage.hive_settings->hdfs_namenode.value);

        auto format = convertHiveFormat(table.sd.serdeInfo.serializationLib);
        auto writer = FormatFactory::instance().getOutput(format, *write_buf, metadata_snapshot->getSampleBlock(), context);

        writer->write(part_block.block);
        writer->flush();

        if (!partition_exists)
        {
            Apache::Hadoop::Hive::Partition partition;
            partition.dbName = storage.hive_database;
            partition.tableName = storage.hive_table;
            partition.sd = table.sd;
            partition.sd.location = uri_without_path + location_dir;
            partition.privileges = table.privileges;
            for (const auto & p : part_block.partition)
            {
                partition.values.push_back(toString(p));
            }
            Apache::Hadoop::Hive::Partition dummpy;
            client.add_partition(dummpy, partition);
        }
    }
}

}

#endif
