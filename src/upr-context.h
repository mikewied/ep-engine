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

#ifndef SRC_UPR_CONTEXT_H_
#define SRC_UPR_CONTEXT_H_ 1

#include "config.h"

class ActiveStream;
class EventuallyPersistentEngine;
class MutationResponse;
class SetVBucketState;
class SnapshotMarker;

class ActiveStreamCtx {
public:
    virtual void scheduleBackfill(ActiveStream* stream,
                                  uint64_t start,
                                  uint64_t end) = 0;

    virtual void setVBucketState() = 0;

    virtual queued_item getItemFromMemory(const std::string& name,
                                          uint64_t& snapEnd) = 0;

    virtual uint64_t registerMemoryCursor(uint64_t regSeqno,
                                          uint64_t endSeqno,
                                          const std::string& name) = 0;

    virtual void removeMemoryCursor(const std::string &name) = 0;

    virtual uint64_t getHighSeqno() = 0;

    virtual uint64_t getNumVBucketItems() = 0;

    virtual uint64_t getNumCheckpointItems(const std::string& name) = 0;

    virtual const char* logHeader() = 0;

    virtual void notify(bool schedule) = 0;
};

class PassiveStreamCtx {
public:
    virtual void processMutation(MutationResponse* mutation) = 0;

    virtual void processDeletion(MutationResponse* deletion) = 0;

    virtual void processMarker(SnapshotMarker* marker) = 0;

    virtual void processSetVBucketState(SetVBucketState* state) = 0;

    virtual uint64_t getVBucketUUID() = 0;

    virtual uint64_t getHighSeqno() = 0;

    virtual const char* logHeader() = 0;

    virtual void notify() = 0;
};

class NotifierStreamCtx {
public:
    virtual uint64_t getHighSeqno() = 0;

    virtual const char* logHeader() = 0;

    virtual void notify(bool schedule) = 0;
};

#endif  // SRC_UPR_CONTEXT_H_
