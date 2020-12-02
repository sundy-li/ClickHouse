#pragma once

#include <thrift/transport/TSocket.h>
#include <thrift/transport/TBufferTransports.h>
#include <thrift/protocol/TBinaryProtocol.h>

#include "ThriftHiveMetastore.h"

namespace DB
{

using namespace apache::thrift::transport;
using namespace apache::thrift::protocol;

using TSocketPtr = std::shared_ptr<TSocket>;
using TBinaryProtocolPtr = std::shared_ptr<TBinaryProtocol>;
using TBufferedTransportPtr = std::shared_ptr<TBufferedTransport>;
using HMSClient = Apache::Hadoop::Hive::ThriftHiveMetastoreClient;

class HMSManager
{
public:
    HMSManager(const std::string & host, const int & port);
    HMSClient & getClient();
    ~HMSManager();
private:
    TSocketPtr socket;
    TBufferedTransportPtr trans;
    TBinaryProtocolPtr proto;
    HMSClient client;
};

}