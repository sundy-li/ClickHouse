#include "HiveMetastoreCommon.h"


namespace DB
{

//      Poco::URI metastore_uri(metastore_url);
// -    auto socket = std::make_shared<TSocket>(metastore_uri.getHost(), metastore_uri.getPort());
// -    auto transport = std::make_shared<TBufferedTransport>(socket);
// -    auto protocol = std::make_shared<TBinaryProtocol>(transport);
// -
// -    Apache::Hadoop::Hive::ThriftHiveMetastoreClient client(protocol);
// -    transport->open();
// +    HMSManager hsm_manager(metastore_uri.getHost(), metastore_uri.getPort());
// +    auto & client = hsm_manager.getClient();

HMSManager::HMSManager(const std::string & host, const int & port)
    : socket(std::make_shared<TSocket>(host, port))
    , trans(std::make_shared<TBufferedTransport>(socket))
    , proto(std::make_shared<TBinaryProtocol>(trans))
    , client(proto)
{
    trans->open();
}

HMSClient & HMSManager::getClient()
{
    return client;
}

HMSManager::~HMSManager()
{
    trans->close();
}

}