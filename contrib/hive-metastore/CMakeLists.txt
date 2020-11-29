set (CMAKE_CXX_STANDARD 17)

set(LIBRARY_DIR ${ClickHouse_SOURCE_DIR}/contrib/hive-metastore)

# === thrift
find_library(THRIFT_LIBRARY thrift)
if(NOT THRIFT_LIBRARY OR NOT THRIFT_SOUCE_DIR)
        set(THRIFT_SOUCE_DIR ${ClickHouse_SOURCE_DIR}/contrib/thrift/lib/cpp)
        # contrib/thrift/lib/cpp/CMakeLists.txt
        set(thriftcpp_SOURCES
        ${THRIFT_SOUCE_DIR}/src/thrift/TApplicationException.cpp
        ${THRIFT_SOUCE_DIR}/src/thrift/TOutput.cpp
        ${THRIFT_SOUCE_DIR}/src/thrift/async/TAsyncChannel.cpp
        ${THRIFT_SOUCE_DIR}/src/thrift/async/TAsyncProtocolProcessor.cpp
        ${THRIFT_SOUCE_DIR}/src/thrift/async/TConcurrentClientSyncInfo.h
        ${THRIFT_SOUCE_DIR}/src/thrift/async/TConcurrentClientSyncInfo.cpp
        ${THRIFT_SOUCE_DIR}/src/thrift/concurrency/ThreadManager.cpp
        ${THRIFT_SOUCE_DIR}/src/thrift/concurrency/TimerManager.cpp
        ${THRIFT_SOUCE_DIR}/src/thrift/concurrency/Util.cpp
        ${THRIFT_SOUCE_DIR}/src/thrift/processor/PeekProcessor.cpp
        ${THRIFT_SOUCE_DIR}/src/thrift/protocol/TBase64Utils.cpp
        ${THRIFT_SOUCE_DIR}/src/thrift/protocol/TDebugProtocol.cpp
        ${THRIFT_SOUCE_DIR}/src/thrift/protocol/TJSONProtocol.cpp
        ${THRIFT_SOUCE_DIR}/src/thrift/protocol/TMultiplexedProtocol.cpp
        ${THRIFT_SOUCE_DIR}/src/thrift/protocol/TProtocol.cpp
        ${THRIFT_SOUCE_DIR}/src/thrift/transport/TTransportException.cpp
        ${THRIFT_SOUCE_DIR}/src/thrift/transport/TFDTransport.cpp
        ${THRIFT_SOUCE_DIR}/src/thrift/transport/TSimpleFileTransport.cpp
        ${THRIFT_SOUCE_DIR}/src/thrift/transport/THttpTransport.cpp
        ${THRIFT_SOUCE_DIR}/src/thrift/transport/THttpClient.cpp
        ${THRIFT_SOUCE_DIR}/src/thrift/transport/THttpServer.cpp
        ${THRIFT_SOUCE_DIR}/src/thrift/transport/TSocket.cpp
        ${THRIFT_SOUCE_DIR}/src/thrift/transport/TSocketPool.cpp
        ${THRIFT_SOUCE_DIR}/src/thrift/transport/TServerSocket.cpp
        ${THRIFT_SOUCE_DIR}/src/thrift/transport/TTransportUtils.cpp
        ${THRIFT_SOUCE_DIR}/src/thrift/transport/TBufferTransports.cpp
        ${THRIFT_SOUCE_DIR}/src/thrift/server/TConnectedClient.cpp
        ${THRIFT_SOUCE_DIR}/src/thrift/server/TServerFramework.cpp
        ${THRIFT_SOUCE_DIR}/src/thrift/server/TSimpleServer.cpp
        ${THRIFT_SOUCE_DIR}/src/thrift/server/TThreadPoolServer.cpp
        ${THRIFT_SOUCE_DIR}/src/thrift/server/TThreadedServer.cpp
        )

        set(thriftcpp_threads_SOURCES
                ${THRIFT_SOUCE_DIR}/src/thrift/concurrency/ThreadFactory.cpp
                ${THRIFT_SOUCE_DIR}/src/thrift/concurrency/Thread.cpp
                ${THRIFT_SOUCE_DIR}/src/thrift/concurrency/Monitor.cpp
                ${THRIFT_SOUCE_DIR}/src/thrift/concurrency/Mutex.cpp
                )

        set(THRIFT_LIBRARY thrift_mine)

        include(${ClickHouse_SOURCE_DIR}/contrib/thrift/build/cmake/ConfigureChecks.cmake) # makes config.h
        add_library(${THRIFT_LIBRARY} ${thriftcpp_SOURCES} ${thriftcpp_threads_SOURCES})
        set_target_properties(${THRIFT_LIBRARY} PROPERTIES CXX_STANDARD 14) # REMOVE after https://github.com/apache/thrift/pull/1641
        target_include_directories(${THRIFT_LIBRARY} SYSTEM PUBLIC ${ClickHouse_SOURCE_DIR}/contrib/thrift/lib/cpp/src)

        ## TODO refactor this
        target_include_directories(${THRIFT_LIBRARY} SYSTEM PUBLIC ${CMAKE_CURRENT_BINARY_DIR}) #for config.h

        target_link_libraries (${THRIFT_LIBRARY} PRIVATE boost::headers_only)
endif()

# include_directories(${LIBRARY_DIR}/if/gen-cpp)
# == hive_meta_store
set(meatstore_SOURCES
        ${LIBRARY_DIR}/if/gen-cpp/FacebookService.cpp
        ${LIBRARY_DIR}/if/gen-cpp/fb303_constants.cpp
        ${LIBRARY_DIR}/if/gen-cpp/fb303_types.cpp
        ${LIBRARY_DIR}/if/gen-cpp/hive_metastore_constants.cpp
        ${LIBRARY_DIR}/if/gen-cpp/hive_metastore_types.cpp
        ${LIBRARY_DIR}/if/gen-cpp/ThriftHiveMetastore.cpp)

add_library(hive-metastore STATIC ${meatstore_SOURCES})
target_link_libraries(hive-metastore PUBLIC thrift_mine)
