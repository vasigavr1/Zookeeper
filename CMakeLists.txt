cmake_minimum_required(VERSION 3.18)
project(odyssey)

set(odyssey_VERSION_MAJOR 1)

include_directories(odyssey-shared/include/libhrd kite/include/kite odyssey-shared/include/mica odyssey-shared/include/general_util include/zookeeper
        kite/include/kite_inline_util /usr/include/)

set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -O3 -std=gnu1x -march=native -Wno-address-of-packed-member \
-Wno-format-truncation -g ")


set(CMAKE_LINKER_FLAGS)


set(COMMON_FILES
        odyssey-shared/src/mica/kvs.c
        #libhrd
        odyssey-shared/include/libhrd/hrd.h
        odyssey-shared/src/libhrd/hrd_util.c
        odyssey-shared/src/libhrd/hrd_conn.c
        #mica
        odyssey-shared/src/mica/city.c
        odyssey-shared/include/mica/kvs.h
        odyssey-shared/include/mica/city.h
        /usr/include/infiniband/verbs.h
        odyssey-shared/src/client/client.c
        odyssey-shared/include/general_util/generic_macros.h
        odyssey-shared/include/general_util/client_if_util.h
        odyssey-shared/include/general_util/interface.h
        odyssey-shared/include/general_util/latency_util.h
        odyssey-shared/include/general_util/stats.h
        odyssey-shared/include/general_util/top.h
        odyssey-shared/include/general_util/init_func.h
        odyssey-shared/include/general_util/generic_inline_util.h
        odyssey-shared/include/general_util/init_connect.h
        odyssey-shared/include/general_util/generic_opcodes.h
        odyssey-shared/include/general_util/trace_util.h
        odyssey-shared/include/general_util/rdma_gen_util.h
        odyssey-shared/include/general_util/debug_util.h
        odyssey-shared/include/general_util/inline_util.h
        odyssey-shared/include/general_util/sizes.h
        odyssey-shared/include/general_util/config_util.h)


set(ZK_SOURCE_FILES
        src/zookeeper/zk_stats.c
        src/zookeeper/zk_util.c
        include/zookeeper/zk_cache.h
        include/zookeeper/zk_inline_util.h
        include/zookeeper/zk_latency.h
        include/zookeeper/zk_bqr.h
        include/zookeeper/zk_main.h
        include/zookeeper/zk_util.h
        include/zookeeper/zk_config.h
        include/zookeeper/zk_kvs_util.h
        include/zookeeper/zk_opcodes.h
        include/zookeeper/zk_debug_util.h
        include/zookeeper/zk_reservation_stations_util.h
        )


add_executable(zookeeper ${ZK_SOURCE_FILES} ${COMMON_FILES})

set_target_properties(zookeeper PROPERTIES COMPILE_FLAGS -DZOOKEEPER)

target_link_libraries(zookeeper pthread ibverbs rt numa rdmacm)
