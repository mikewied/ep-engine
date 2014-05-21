/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2013 Couchbase, Inc
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

#include "upr-context.h"

class ActiveStream;
class EventuallyPersistentEngine;
class UprProducer;

class ActiveStreamEngineCtx : public ActiveStreamCtx {
public:

    ActiveStreamEngineCtx(EventuallyPersistentEngine* engine,
                          UprProducer* producer, uint16_t vbid);

    void scheduleBackfill(ActiveStream* stream, uint64_t start, uint64_t end);

    void setVBucketState();

    queued_item getItemFromMemory(const std::string&name, uint64_t& snapEnd);

    uint64_t registerMemoryCursor(uint64_t regSeqno, uint64_t endSeqno,
                                  const std::string& name);

    void removeMemoryCursor(const std::string &name);

    uint64_t getHighSeqno();

    uint64_t getNumVBucketItems();

    uint64_t getNumCheckpointItems(const std::string& name);

    const char* logHeader();

    void notify(bool schedule);

private:
    EventuallyPersistentEngine* engine;
    UprProducer* producer;
    uint16_t vbid;
};
