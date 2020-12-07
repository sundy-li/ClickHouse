#include <Common/config.h>
#include "registerTableFunctions.h"

#if USE_HDFS
#    include <DataTypes/DataTypeDate.h>
#    include <DataTypes/DataTypeDateTime.h>
#    include <DataTypes/DataTypeDateTime64.h>
#    include <DataTypes/DataTypeEnum.h>
#    include <DataTypes/DataTypeFixedString.h>
#    include <DataTypes/DataTypeNullable.h>
#    include <DataTypes/DataTypeString.h>
#    include <DataTypes/DataTypesDecimal.h>
#    include <DataTypes/DataTypesNumber.h>
#    include <DataTypes/IDataType.h>
#    include <Interpreters/evaluateConstantExpression.h>
#    include <Parsers/ASTFunction.h>
#    include <Parsers/ASTIdentifier.h>
#    include <Parsers/ASTLiteral.h>
#    include <Storages/ColumnsDescription.h>
#    include <Storages/Hive/StorageHive.h>
#    include <TableFunctions/TableFunctionFactory.h>
#    include <TableFunctions/TableFunctionHive.h>

#    include <boost/algorithm/string.hpp>
#    include <Poco/URI.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

void TableFunctionHive::parseArguments(const ASTPtr & ast_function, const Context & context)
{
    const auto & args_func = ast_function->as<ASTFunction &>();
    if (!args_func.arguments)
        throw Exception("Table function '" + getName() + "' must have arguments", ErrorCodes::LOGICAL_ERROR);

    ASTs & args = args_func.arguments->children;
    if (args.size() != 3 && args.size() != 4)
        throw Exception(
            "Table function '" + getName()
                + "'requires 3 or 4 arguments: HiveMetastore URI, Hive DBName, Hive TableName, and optional HDFS URI",
            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

    args[0] = evaluateConstantExpressionOrIdentifierAsLiteral(args[0], context);
    args[1] = evaluateConstantExpressionOrIdentifierAsLiteral(args[1], context);
    args[2] = evaluateConstantExpressionOrIdentifierAsLiteral(args[2], context);

    metastore_uri = args[0]->as<ASTLiteral &>().value.safeGet<String>();
    hive_database = args[1]->as<ASTLiteral &>().value.safeGet<String>();
    hive_table = args[2]->as<ASTLiteral &>().value.safeGet<String>();

    if (args.size() == 4)
    {
        hdfs_uri = evaluateConstantExpressionOrIdentifierAsLiteral(args[3], context)->as<ASTLiteral &>().value.safeGet<String>();
    }
}

ColumnsDescription TableFunctionHive::getActualTableStructure(const Context & /*context*/) const
{
    Poco::URI uri(metastore_uri);
    auto manager = HMSManager(uri.getHost(), uri.getPort());
    auto & client = manager.getClient();
    client.get_table(table, hive_database, hive_table);

    ColumnsDescription res;
    for (const auto & col : table.sd.cols)
    {
        res.add(ColumnDescription(col.name, convertHiveDataType(col.type, true)));
    }

    for (const auto & col : table.partitionKeys)
    {
        res.add(ColumnDescription(col.name, convertHiveDataType(col.type, false)));
    }
    return res;
}

StoragePtr TableFunctionHive::executeImpl(
    const ASTPtr & /*ast_function*/, const Context & context, const std::string & table_name, ColumnsDescription /*cached_columns*/) const
{
    auto columns = getActualTableStructure(context);
    if (!table.partitionKeys.empty())
    {
        ASTs children;
        for (const auto & c : table.partitionKeys)
        {
            children.emplace_back(std::make_shared<ASTIdentifier>(c.name));
        }
        partition_by_ast = makeASTFunction("tuple", children);
    }

    auto hive_settings = std::make_unique<HiveSettings>();
    if (!hdfs_uri.empty())
        hive_settings->hdfs_namenode = hdfs_uri;
    auto res = StorageHive::create(
        StorageID(getDatabaseName(), table_name),
        metastore_uri,
        hive_database,
        hive_table,
        columns,
        std::move(hive_settings),
        ConstraintsDescription{},
        partition_by_ast,
        context);
    res->startup();
    return res;
}

// from https://cwiki.apache.org/confluence/display/Hive/LanguageManual+Types
std::unordered_map<String, DataTypePtr> TableFunctionHive::type_mappers = {
    // Numeric Types
    {"TINYINT", std::make_shared<DataTypeInt8>()},
    {"SMALLINT", std::make_shared<DataTypeInt16>()},
    {"INT", std::make_shared<DataTypeInt32>()},
    {"BIGINT", std::make_shared<DataTypeInt64>()},
    {"FLOAT", std::make_shared<DataTypeFloat32>()},
    {"DOUBLE", std::make_shared<DataTypeFloat64>()},

    {"DECIMAL", createDecimalMaxPrecision<Decimal64>(18)},
    {"DOUBLE PRECISION", createDecimalMaxPrecision<Decimal64>(18)},

    // Data/Time Types
    {"TIMESTAMP", std::make_shared<DataTypeDateTime64>(9)},
    {"Date", std::make_shared<DataTypeDate>()},

    // String
    {"STRING", std::make_shared<DataTypeString>()},
    {"VARCHAR", std::make_shared<DataTypeString>()},
    {"CHAR", std::make_shared<DataTypeString>()},

    // Misc Types
    {"BOOLEAN", std::make_shared<DataTypeUInt8>()},
    {"BINARY", std::make_shared<DataTypeString>()},
};

DataTypePtr TableFunctionHive::convertHiveDataType(const String & type_name, bool is_null) const
{
    String upper_type_name{type_name};
    boost::to_upper(upper_type_name);
    DataTypePtr res;
    auto it = type_mappers.find(upper_type_name);
    if (it == type_mappers.end())
        throw Exception("DatType " + type_name + " currently not supported.", ErrorCodes::LOGICAL_ERROR);

    res = it->second;
    if (is_null)
        res = std::make_shared<DataTypeNullable>(res);
    return res;
}

void registerTableFunctionHIVE(TableFunctionFactory & factory)
{
    factory.registerFunction<TableFunctionHive>();
}

}
#endif
