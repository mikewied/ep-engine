/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2012 Couchbase, Inc
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

#include "config.h"

#include <memcached/engine.h>

#include <string>
#include <utility>

#include "ep_engine.h"
#include "memory_tracker.h"
#include "objectregistry.h"

int MemoryTracker::times = 0;
bool MemoryTracker::tracking = false;
MemoryTracker *MemoryTracker::instance = NULL;

extern "C" {
    static void updateStatsThread(void* arg) {
        MemoryTracker* tracker = static_cast<MemoryTracker*>(arg);
        while (tracker->trackingMemoryAllocations()) {
            tracker->updateStats();
            usleep(250000);
        }
    }
}

MemoryTracker *MemoryTracker::getInstance() {
    if (!instance) {
        instance = new MemoryTracker();
    }
    return instance;
}

extern "C" {
    static void NewHook(const void* ptr, size_t) {
        if (ptr != NULL) {
            void* p = const_cast<void*>(ptr);

            size_t alloc = getHooksApi()->get_allocation_size(p);
            ObjectRegistry::memoryAllocated(alloc);

            EventuallyPersistentEngine* eng = ObjectRegistry::getCurrentEngine();
            uint8_t* specific = static_cast<uint8_t*>(p) + (alloc - 8);
            memcpy(specific, &eng, 8);
        }
    }

    static void DeleteHook(const void* ptr) {
        if (ptr != NULL) {
            void* p = const_cast<void*>(ptr);
            size_t alloc = getHooksApi()->get_allocation_size(p);
            ObjectRegistry::memoryDeallocated(alloc);

            uint8_t* specific = static_cast<uint8_t*>(p) + (alloc - 8);
            EventuallyPersistentEngine* eng = NULL;
            EventuallyPersistentEngine* cur = ObjectRegistry::getCurrentEngine();
            memcpy(&eng, specific, 8);
            if (eng != cur) {
                const char* aloc_eng = eng ? eng->getName() : "memcached";
                const char* free_eng = cur ? cur->getName() : "memcached";
                LOG(EXTENSION_LOG_WARNING, "Invalid free of size %llu, "
                    "Allocated in %s, freed in %s\n", alloc, aloc_eng, free_eng);
                if (alloc == 48) {
                    MemoryTracker::times++;
                }
                if (MemoryTracker::times > 200) {
                    abort();
                }
            }
        }
    }
}

MemoryTracker::MemoryTracker() {
    if (getenv("EP_NO_MEMACCOUNT") != NULL) {
        LOG(EXTENSION_LOG_WARNING, "Memory allocation tracking disabled");
        return;
    }
    stats.ext_stats_size = getHooksApi()->get_extra_stats_size();
    stats.ext_stats = (allocator_ext_stat*) calloc(stats.ext_stats_size,
                                                   sizeof(allocator_ext_stat));
    if (getHooksApi()->add_new_hook(&NewHook)) {
        LOG(EXTENSION_LOG_DEBUG, "Registered add hook");
        if (getHooksApi()->add_delete_hook(&DeleteHook)) {
            LOG(EXTENSION_LOG_DEBUG, "Registered delete hook");
            std::cout.flush();
            tracking = true;
            updateStats();
            if (cb_create_thread(&statsThreadId, updateStatsThread, this, 0)
                != 0) {
                throw std::runtime_error(
                                      "Error creating thread to update stats");
            }
            return;
        }
        std::cout.flush();
        getHooksApi()->remove_new_hook(&NewHook);
    }
    LOG(EXTENSION_LOG_WARNING, "Failed to register allocator hooks");
}

MemoryTracker::~MemoryTracker() {
    getHooksApi()->remove_new_hook(&NewHook);
    getHooksApi()->remove_delete_hook(&DeleteHook);
    if (tracking) {
        tracking = false;
        cb_join_thread(statsThreadId);
    }
    instance = NULL;
}

void MemoryTracker::getAllocatorStats(std::map<std::string, size_t>
                                                               &alloc_stats) {
    if (!trackingMemoryAllocations()) {
        return;
    }

    for (size_t i = 0; i < stats.ext_stats_size; ++i) {
        alloc_stats.insert(std::pair<std::string, size_t>(
                                                    stats.ext_stats[i].key,
                                                    stats.ext_stats[i].value));
    }
    alloc_stats.insert(std::make_pair("total_allocated_bytes",
                                      stats.allocated_size));
    alloc_stats.insert(std::make_pair("total_heap_bytes",
                                      stats.heap_size));
    alloc_stats.insert(std::make_pair("total_free_mapped_bytes",
                                      stats.free_mapped_size));
    alloc_stats.insert(std::make_pair("total_free_unmapped_bytes",
                                      stats.free_unmapped_size));
    alloc_stats.insert(std::make_pair("total_fragmentation_bytes",
                                      stats.fragmentation_size));
}

void MemoryTracker::getDetailedStats(char* buffer, int size) {
    getHooksApi()->get_detailed_stats(buffer, size);
}

void MemoryTracker::updateStats() {
    getHooksApi()->get_allocator_stats(&stats);
}

size_t MemoryTracker::getFragmentation() {
    return stats.fragmentation_size;
}

size_t MemoryTracker::getTotalBytesAllocated() {
    return stats.allocated_size;
}

size_t MemoryTracker::getTotalHeapBytes() {
    return stats.heap_size;
}

bool MemoryTracker::trackingMemoryAllocations() {
    return tracking;
}
