#include <Common/config.h>

#if USE_HDFS

#include <Storages/StorageFactory.h>
#include <Storages/Hive/StorageHive.h>
#include <Storages/Hive/HiveBlockOutputStream.h>
#include <Storages/MergeTree/KeyCondition.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Interpreters/Context.h>
#include <Interpreters/ActionsDAG.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Parsers/ASTLiteral.h>
#include <Storages/MergeTree/PartitionPruner.h>
#include <IO/ReadHelpers.h>
#include <IO/ReadBufferFromHDFS.h>
#include <IO/WriteBufferFromString.h>
#include <IO/WriteHelpers.h>
#include <IO/HDFSCommon.h>
#include <Formats/FormatFactory.h>
#include <Formats/FormatSettings.h>
#include <DataTypes/DataTypeString.h>
#include <DataStreams/IBlockOutputStream.h>
#include <DataStreams/OwningBlockInputStream.h>
#include <DataStreams/IBlockInputStream.h>
#include <common/logger_useful.h>
#include <Common/parseGlobs.h>

#include <Poco/URI.h>
#include <re2/re2.h>
#include <re2/stringpiece.h>
#include <hdfs/hdfs.h>
#include <Processors/Sources/SourceWithProgress.h>
#include <Processors/Pipe.h>
#include <fmt/format.h>

#include <metastore/HiveMetastoreCommon.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int INVALID_PARTITION_VALUE;
    extern const int FORMAT_IS_NOT_SUITABLE_FOR_INPUT;
}

StorageHive::StorageHive(const StorageID & table_id_,
    const String & metastore_url_,
    const String & hive_database_,
    const String & hive_table_,
    const ColumnsDescription & columns_,
    std::unique_ptr<HiveSettings> hive_settings_,
    const ConstraintsDescription & constraints_,
    const ASTPtr & partition_by_ast_,
    const Context & context_)
    : IStorage(table_id_)
    , metastore_url(metastore_url_)
    , hive_database(hive_database_)
    , hive_table(hive_table_)
    , hive_settings(std::move(hive_settings_))
    , partition_by_ast(partition_by_ast_)
    , context(context_)
    , timeouts{ConnectionTimeouts::getHTTPTimeouts(context)}
{
    StorageInMemoryMetadata storage_metadata;
    storage_metadata.setColumns(columns_);
    storage_metadata.setConstraints(constraints_);

    if (partition_by_ast)
    {
        storage_metadata.partition_key = KeyDescription::getKeyFromAST(partition_by_ast, columns_, context);

        partition_name_types = storage_metadata.partition_key.expression->getRequiredColumnsWithTypes();
        minmax_idx_expr = std::make_shared<ExpressionActions>(std::make_shared<ActionsDAG>(partition_name_types));
    }
    setInMemoryMetadata(storage_metadata);
}


namespace
{

struct PartitonWithFile
{
    FieldVector values;
    String file;
    PartitonWithFile(const FieldVector & values_, const String & file_)
    : values(values_)
    , file(file_)
    {}
};

using PartitonWithFiles = std::vector<PartitonWithFile>;

class HiveSource : public SourceWithProgress
{
public:
    struct SourcesInfo
    {
        PartitonWithFiles partition_files;
        NamesAndTypesList partition_name_types;

        std::atomic<size_t> next_uri_to_read = 0;

        bool need_path_column = false;
        bool need_file_column = false;
    };

    using SourcesInfoPtr = std::shared_ptr<SourcesInfo>;

    static Block getHeader(Block header, const SourcesInfoPtr & source_info)
    {
        if (source_info->need_path_column)
            header.insert({DataTypeString().createColumn(), std::make_shared<DataTypeString>(), "_path"});
        if (source_info->need_file_column)
            header.insert({DataTypeString().createColumn(), std::make_shared<DataTypeString>(), "_file"});

        return header;
    }

    HiveSource(
        SourcesInfoPtr source_info_,
        String uri_,
        String format_,
        String compression_method_,
        String hdfs_namenode_,
        Block sample_block_,
        const Context & context_,
        UInt64 max_block_size_)
        : SourceWithProgress(getHeader(sample_block_, source_info_))
        , source_info(std::move(source_info_))
        , uri(uri_)
        , format(std::move(format_))
        , compression_method(compression_method_)
        , hdfs_namenode(hdfs_namenode_)
        , max_block_size(max_block_size_)
        , sample_block(std::move(sample_block_))
        , context(context_)
    {
    }

    String getName() const override
    {
        return "Hive";
    }

    Chunk generate() override
    {
        std::vector<size_t> partition_indexs;
        auto to_read_block = sample_block;
        for (const auto & name_type : source_info->partition_name_types)
        {
            to_read_block.erase(name_type.name);
        }

        while (true)
        {
            if (!reader)
            {
                current_idx = source_info->next_uri_to_read.fetch_add(1);
                if (current_idx >= source_info->partition_files.size())
                    return {};

                current_path = source_info->partition_files[current_idx].file;

                String uri_with_path = uri + current_path;
                auto compression = chooseCompressionMethod(current_path, compression_method);
                auto read_buf = wrapReadBufferWithCompressionMethod(std::make_unique<ReadBufferFromHDFS>(uri_with_path, hdfs_namenode), compression);
                auto input_stream = FormatFactory::instance().getInput(format, *read_buf, to_read_block, context, max_block_size);

                reader = std::make_shared<OwningBlockInputStream<ReadBuffer>>(input_stream, std::move(read_buf));
                reader->readPrefix();
            }

            if (auto res = reader->read())
            {
                Columns columns = res.getColumns();
                UInt64 num_rows = res.rows();
                auto types = source_info->partition_name_types.getTypes();
                for (size_t i = 0; i < types.size(); ++i)
                {
                    auto column = types[i]->createColumnConst(num_rows, source_info->partition_files[current_idx].values[i]);
                    auto previous_idx = sample_block.getPositionByName(source_info->partition_name_types.getNames()[i]);
                    columns.insert(columns.begin() + previous_idx, column->convertToFullColumnIfConst());
                }

                  /// Enrich with virtual columns.
                if (source_info->need_path_column)
                {
                    auto column = DataTypeString().createColumnConst(num_rows, current_path);
                    columns.push_back(column->convertToFullColumnIfConst());
                }

                if (source_info->need_file_column)
                {
                    size_t last_slash_pos = current_path.find_last_of('/');
                    auto file_name = current_path.substr(last_slash_pos + 1);

                    auto column = DataTypeString().createColumnConst(num_rows, std::move(file_name));
                    columns.push_back(column->convertToFullColumnIfConst());
                }

                return Chunk(std::move(columns), num_rows);
            }

            reader->readSuffix();
            reader.reset();
        }
    }

private:
    BlockInputStreamPtr reader;
    SourcesInfoPtr source_info;
    String uri;
    String format;
    String compression_method;
    String hdfs_namenode;
    UInt64 max_block_size;
    Block sample_block;

    String current_path;
    size_t current_idx = 0;
    const Context & context;
};

}

String convertHiveFormat(const String & hive_format)
{
    //currently only support Parquet, ORC
    static std::unordered_map<String, String> format_map = {
        {"org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe", "Parquet"},
        {"org.apache.hadoop.hive.ql.io.orc.OrcSerde", "ORC"},
    };

    auto it = format_map.find(hive_format);
    if (it == format_map.end())
        throw Exception("Format " + hive_format + " is not suitable for input", ErrorCodes::FORMAT_IS_NOT_SUITABLE_FOR_INPUT);
    return it->second;
}


Pipe StorageHive::read(
    const Names & column_names,
    const StorageMetadataPtr & metadata_snapshot,
    SelectQueryInfo & query_info,
    const Context & context_,
    QueryProcessingStage::Enum /*processed_stage*/,
    size_t max_block_size,
    unsigned num_streams)
{
    Poco::URI metastore_uri(metastore_url);
    HMSManager hsm_manager(metastore_uri.getHost(), metastore_uri.getPort());
    auto & client = hsm_manager.getClient();

    Apache::Hadoop::Hive::Table table;
    std::vector<Apache::Hadoop::Hive::Partition> partitions;
    PartitonWithFiles partition_files;

    client.get_table(table, hive_database, hive_table);
    String location = table.sd.location;
    const size_t begin_of_path = location.find('/', location.find("//") + 2);
    String uri_without_path = location.substr(0, begin_of_path);
    HDFSBuilderPtr builder = createHDFSBuilder(uri_without_path + "/", hive_settings->hdfs_namenode.value);
    HDFSFSPtr fs = createHDFSFS(builder.get());
    FieldVector fields(partition_name_types.size());

    auto list_files = [&fs](const String & file_location)
    {
        Strings files;
        Poco::URI location_uri(file_location);
        HDFSFileInfo ls;
        ls.file_info = hdfsListDirectory(fs.get(), location_uri.getPath().c_str(), &ls.length);
        for (int i = 0; i < ls.length; ++i)
        {
            if (ls.file_info[i].mKind != 'D' && ls.file_info[i].mSize > 0)
            {
                files.push_back(String(ls.file_info[i].mName));
            }
        }
        return files;
    };

    // path for table with partition expr
    if (minmax_idx_expr)
    {
        client.get_partitions(partitions, hive_database, hive_table, -1);
        if (partitions.size() == 0)
            return {};

        const auto names = partition_name_types.getNames();
        const auto types = partition_name_types.getTypes();
        std::optional<KeyCondition> minmax_idx_condition;
        minmax_idx_condition.emplace(query_info, context, names, minmax_idx_expr);

        for (const auto & p : partitions)
        {
            std::vector<Range> ranges;
            WriteBufferFromOwnString wb;
            if (p.values.size() != names.size())
                throw Exception(fmt::format("Partition value size not match, expect {}, but got {}", names.size(), p.values.size()),
                    ErrorCodes::INVALID_PARTITION_VALUE);

            for (size_t i = 0; i < p.values.size(); ++i)
            {
                if (i != 0)
                    writeString(",", wb);
                writeString(p.values[i], wb);
            }
            ReadBufferFromString buffer(wb.str());
            auto input_stream
                = FormatFactory::instance().getInput("CSV", buffer, metadata_snapshot->getPartitionKey().sample_block, context, context.getSettingsRef().max_block_size);

            auto block = input_stream->read();
            if (!block || !block.rows())
                throw Exception(
                    "Could not parse partition value: " + wb.str(),
                    ErrorCodes::INVALID_PARTITION_VALUE);

            for (size_t i = 0; i < names.size(); ++i)
            {
                block.getByPosition(i).column->get(0, fields[i]);
                ranges.emplace_back(fields[i]);
            }

            if (!minmax_idx_condition->checkInHyperrectangle(ranges, partition_name_types.getTypes()).can_be_true)
                continue;

            auto files = list_files(p.sd.location);
            for (const auto & file : files)
            {
                partition_files.emplace_back(fields, file);
            }
        }
    }
    else
    {
        auto files = list_files(table.sd.location);
        for (const auto & file : files)
        {
            partition_files.emplace_back(fields, file);
        }
    }


    auto sources_info = std::make_shared<HiveSource::SourcesInfo>();
    sources_info->partition_files = std::move(partition_files);
    sources_info->partition_name_types = partition_name_types;

    for (const auto & column : column_names)
    {
        if (column == "_path")
            sources_info->need_path_column = true;
        if (column == "_file")
            sources_info->need_file_column = true;
    }

    if (num_streams > sources_info->partition_files.size())
        num_streams = sources_info->partition_files.size();

    Pipes pipes;
    for (size_t i = 0; i < num_streams; ++i)
        pipes.emplace_back(std::make_shared<HiveSource>(
                sources_info, uri_without_path, convertHiveFormat(table.sd.serdeInfo.serializationLib), "auto",
                hive_settings->hdfs_namenode.value, metadata_snapshot->getSampleBlock(), context_, max_block_size));

    return Pipe::unitePipes(std::move(pipes));
}

BlockOutputStreamPtr StorageHive::write(const ASTPtr & /*query*/, const StorageMetadataPtr & metadata_snapshot, const Context & context_)
{
    return std::make_shared<HiveBlockOutputStream>(*this, metadata_snapshot, context_, context.getSettings().max_insert_block_size);
}


// Though partition cols is virtual column of hdfs storage
// but we can consider it as material column in ClickHouse
NamesAndTypesList StorageHive::getVirtuals() const
{
    return NamesAndTypesList{
        {"_path", std::make_shared<DataTypeString>()},
        {"_file", std::make_shared<DataTypeString>()}
    };
}

void registerStorageHive(StorageFactory & factory)
{
    StorageFactory::StorageFeatures features{
        .supports_settings = true,
        .supports_sort_order = true,
        .source_access_type = AccessType::HDFS
    };

    factory.registerStorage("Hive", [](const StorageFactory::Arguments & args)
    {
        ASTs & engine_args = args.engine_args;

        if (engine_args.size() != 3)
            throw Exception(
                "Storage Hive requires one argument1: metastore uri.", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        engine_args[0] = evaluateConstantExpressionOrIdentifierAsLiteral(engine_args[0], args.local_context);
        engine_args[1] = evaluateConstantExpressionOrIdentifierAsLiteral(engine_args[1], args.local_context);
        engine_args[2] = evaluateConstantExpressionOrIdentifierAsLiteral(engine_args[2], args.local_context);

        String metastore_url = engine_args[0]->as<ASTLiteral &>().value.safeGet<String>();
        String hive_database = engine_args[1]->as<ASTLiteral &>().value.safeGet<String>();
        String hive_table = engine_args[2]->as<ASTLiteral &>().value.safeGet<String>();

        bool has_settings = args.storage_def->settings;
        auto hive_settings = std::make_unique<HiveSettings>();
        if (has_settings)
        {
            hive_settings->loadFromQuery(*args.storage_def);
        }


        ASTPtr partition_by_ast;
        if (args.storage_def->partition_by)
            partition_by_ast = args.storage_def->partition_by->ptr();

        return StorageHive::create(args.table_id, metastore_url, hive_database, hive_table, args.columns, (std::move(hive_settings)), args.constraints, partition_by_ast, args.context);
    }, features);
}

} //end of DB

#endif
