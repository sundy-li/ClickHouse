#include "HiveMetastoreCommon.h"

namespace DB
{

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