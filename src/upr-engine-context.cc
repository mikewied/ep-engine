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

#include <string>

#include "ep_engine.h"
#include "failover-table.h"
#include "upr-engine-context.h"
#include "upr-consumer.h"
#include "upr-producer.h"
#include "upr-response.h"
#include "upr-stream.h"

#define UPR_BACKFILL_SLEEP_TIME 2

class SnapshotMarkerCallback : public Callback<SeqnoRange> {
public:
    SnapshotMarkerCallback(stream_t s)
        : stream(s) {
        assert(s->getType() == STREAM_ACTIVE);
    }

    void callback(SeqnoRange &range) {
        uint64_t st = range.getStartSeqno();
        uint64_t en = range.getEndSeqno();
        static_cast<ActiveStream*>(stream.get())->markDiskSnapshot(st, en);
    }

private:
    stream_t stream;
};

class CacheCallback : public Callback<CacheLookup> {
public:
    CacheCallback(EventuallyPersistentEngine* e, stream_t &s)
        : engine_(e), stream_(s) {
        Stream *str = stream_.get();
        if (str) {
            assert(str->getType() == STREAM_ACTIVE);
        }
    }

    void callback(CacheLookup &lookup);

private:
    EventuallyPersistentEngine* engine_;
    stream_t stream_;
};

void CacheCallback::callback(CacheLookup &lookup) {
    RCPtr<VBucket> vb = engine_->getEpStore()->getVBucket(lookup.getVBucketId());
    if (!vb) {
        setStatus(ENGINE_SUCCESS);
        return;
    }

    int bucket_num(0);
    LockHolder lh = vb->ht.getLockedBucket(lookup.getKey(), &bucket_num);
    StoredValue *v = vb->ht.unlocked_find(lookup.getKey(), bucket_num, false, false);
    if (v && v->isResident() && v->getBySeqno() == lookup.getBySeqno()) {
        Item* it = v->toItem(false, lookup.getVBucketId());
        lh.unlock();
        static_cast<ActiveStream*>(stream_.get())->backfillReceived(it);
        setStatus(ENGINE_KEY_EEXISTS);
    } else {
        setStatus(ENGINE_SUCCESS);
    }
}

class DiskCallback : public Callback<GetValue> {
public:
    DiskCallback(stream_t &s)
        : stream_(s) {
        Stream *str = stream_.get();
        if (str) {
            assert(str->getType() == STREAM_ACTIVE);
        }
    }

    void callback(GetValue &val) {
        cb_assert(val.getValue());
        ActiveStream* active_stream = static_cast<ActiveStream*>(stream_.get());
        active_stream->backfillReceived(val.getValue());
    }

private:
    stream_t stream_;
};

class UprBackfill : public GlobalTask {
public:
    UprBackfill(EventuallyPersistentEngine* e, stream_t s,
                uint64_t start_seqno, uint64_t end_seqno, const Priority &p,
                double sleeptime = 0, bool shutdown = false)
        : GlobalTask(e, p, sleeptime, shutdown), engine(e), stream(s),
          startSeqno(start_seqno), endSeqno(end_seqno) {
        assert(stream->getType() == STREAM_ACTIVE);
    }

    bool run();

    std::string getDescription();

private:
    EventuallyPersistentEngine *engine;
    stream_t                    stream;
    uint64_t                    startSeqno;
    uint64_t                    endSeqno;
};

bool UprBackfill::run() {
    uint16_t vbid = stream->getVBucket();
    uint64_t lastPersistedSeqno =
        engine->getEpStore()->getVBuckets().getPersistenceSeqno(vbid);

    if (lastPersistedSeqno < endSeqno) {
        LOG(EXTENSION_LOG_WARNING, "Rescheduling backfill for vbucket %d "
            "because backfill up to seqno %llu is needed but only up to "
            "%llu is persisted", vbid, endSeqno,
            lastPersistedSeqno);
        snooze(UPR_BACKFILL_SLEEP_TIME);
        return true;
    }

    KVStore* kvstore = engine->getEpStore()->getAuxUnderlying();
    size_t numItems = kvstore->getNumItems(stream->getVBucket(), startSeqno,
                                           std::numeric_limits<uint64_t>::max());
    static_cast<ActiveStream*>(stream.get())->incrBackfillRemaining(numItems);

    if (numItems > 0) {
        shared_ptr<Callback<GetValue> >
            cb(new DiskCallback(stream));
        shared_ptr<Callback<CacheLookup> >
            cl(new CacheCallback(engine, stream));
        shared_ptr<Callback<SeqnoRange> >
            sr(new SnapshotMarkerCallback(stream));
        kvstore->dump(vbid, startSeqno, cb, cl, sr);
    }

    static_cast<ActiveStream*>(stream.get())->completeBackfill();

    return false;
}

std::string UprBackfill::getDescription() {
    std::stringstream ss;
    ss << "Upr backfill for vbucket " << stream->getVBucket();
    return ss.str();
}

ActiveStreamEngineCtx::ActiveStreamEngineCtx(EventuallyPersistentEngine* e,
                                             UprProducer* p,
                                             uint16_t vb)
    : ActiveStreamCtx(), engine(e), producer(p), vbid(vb) {

}

void ActiveStreamEngineCtx::scheduleBackfill(ActiveStream* stream,
                                             uint64_t start, uint64_t end) {
    ExTask task = new UprBackfill(engine, stream, start, end,
                                  Priority::TapBgFetcherPriority, 0, false);
    ExecutorPool::get()->schedule(task, AUXIO_TASK_IDX);
}

void ActiveStreamEngineCtx::setVBucketState() {
    engine->getEpStore()->setVBucketState(vbid, vbucket_state_dead, false,
                                          false);
}

queued_item ActiveStreamEngineCtx::getItemFromMemory(const std::string& name,
                                                     uint64_t& snapEnd) {
    RCPtr<VBucket> vb = engine->getVBucket(vbid);
    // TODO: Check for no vbucket

    bool isLast;
    queued_item qi = vb->checkpointManager.nextItem(name, isLast, snapEnd);

    if (qi->getOperation() == queue_op_checkpoint_end) {
        qi = vb->checkpointManager.nextItem(name, isLast, snapEnd);
    }
    return qi;
}

uint64_t ActiveStreamEngineCtx::registerMemoryCursor(uint64_t regSeqno,
                                                     uint64_t endSeqno,
                                                     const std::string& name) {
    RCPtr<VBucket> vb = engine->getVBucket(vbid);
    if (!vb) {
        return std::numeric_limits<uint64_t>::max();
    }
    return vb->checkpointManager.registerTAPCursorBySeqno(name, regSeqno,
                                                          endSeqno);
}

void ActiveStreamEngineCtx::removeMemoryCursor(const std::string &name) {
    RCPtr<VBucket> vb = engine->getVBucket(vbid);
    if (vb) {
        vb->checkpointManager.removeTAPCursor(name);
    }
}

uint64_t ActiveStreamEngineCtx::getNumVBucketItems() {
    RCPtr<VBucket> vb = engine->getVBucket(vbid);
    if (!vb) {
        return 0;
    }

    item_eviction_policy_t iep = engine->getEpStore()->getItemEvictionPolicy();
    return vb->getNumItems(iep);
}

uint64_t ActiveStreamEngineCtx::getNumCheckpointItems(const std::string& name) {
    RCPtr<VBucket> vb = engine->getVBucket(vbid);
    if (!vb) {
        return 0;
    }

    return vb->checkpointManager.getNumItemsForTAPConnection(name);
}

uint64_t ActiveStreamEngineCtx::getHighSeqno() {
    RCPtr<VBucket> vb = engine->getVBucket(vbid);
    if (!vb) {
        return 0;
    }
    return vb->getHighSeqno();
}

const char* ActiveStreamEngineCtx::logHeader() {
    return producer->logHeader();
}

void ActiveStreamEngineCtx::notify(bool schedule) {
    producer->notifyStreamReady(vbid, schedule);
}

PassiveStreamEngineCtx::PassiveStreamEngineCtx(EventuallyPersistentEngine* e,
                                               UprConsumer* c, uint16_t vb)
    : PassiveStreamCtx(), engine(e), consumer(c), vbid(vb) {

}

void PassiveStreamEngineCtx::processMutation(MutationResponse* mutation) {
    RCPtr<VBucket> vb = engine->getVBucket(vbid);
    if (!vb) {
        return;
    }

    ENGINE_ERROR_CODE ret;
    if (vb->isBackfillPhase()) {
        ret = engine->getEpStore()->addTAPBackfillItem(*mutation->getItem(),
                                                       INITIAL_NRU_VALUE);
    } else {
        ret = engine->getEpStore()->setWithMeta(*mutation->getItem(), 0,
                                                consumer->getCookie(), true,
                                                true, INITIAL_NRU_VALUE, false);
    }

    delete mutation->getItem();
    delete mutation;

    // We should probably handle these error codes in a better way, but since
    // the producer side doesn't do anything with them anyways let's just log
    // them for now until we come up with a better solution.
    if (ret != ENGINE_SUCCESS) {
        LOG(EXTENSION_LOG_WARNING, "%s Got an error code %d while trying to "
            "process  mutation", consumer->logHeader(), ret);
    }
}

void PassiveStreamEngineCtx::processDeletion(MutationResponse* deletion) {
    RCPtr<VBucket> vb = engine->getVBucket(vbid);
    if (!vb) {
        return;
    }

    uint64_t delCas = 0;
    ENGINE_ERROR_CODE ret;
    ItemMetaData meta = deletion->getItem()->getMetaData();
    ret = engine->getEpStore()->deleteWithMeta(deletion->getItem()->getKey(),
                                               &delCas, deletion->getVBucket(),
                                               consumer->getCookie(), true,
                                               &meta, vb->isBackfillPhase(),
                                               false, deletion->getBySeqno());
    if (ret == ENGINE_KEY_ENOENT) {
        ret = ENGINE_SUCCESS;
    }

    delete deletion->getItem();
    delete deletion;

    // We should probably handle these error codes in a better way, but since
    // the producer side doesn't do anything with them anyways let's just log
    // them for now until we come up with a better solution.
    if (ret != ENGINE_SUCCESS) {
        LOG(EXTENSION_LOG_WARNING, "%s Got an error code %d while trying to "
            "process  deletion", consumer->logHeader(), ret);
    }
}

void PassiveStreamEngineCtx::processMarker(SnapshotMarker* marker) {
    RCPtr<VBucket> vb = engine->getVBucket(vbid);
    if (!vb) {
        return;
    }

    if (marker->getFlags() & MARKER_FLAG_DISK && vb->getHighSeqno() == 0) {
        vb->setBackfillPhase(true);
        vb->checkpointManager.checkAndAddNewCheckpoint(0, vb);
    } else {
        vb->setBackfillPhase(false);
        uint64_t id = vb->checkpointManager.getOpenCheckpointId() + 1;
        vb->checkpointManager.checkAndAddNewCheckpoint(id, vb);
    }
}

void PassiveStreamEngineCtx::processSetVBucketState(SetVBucketState* state) {
    engine->getEpStore()->setVBucketState(vbid, state->getState(), false);
    delete state;
}

uint64_t PassiveStreamEngineCtx::getVBucketUUID() {
    RCPtr<VBucket> vb = engine->getVBucket(vbid);
    if (!vb) {
        return 0;
    }
    return vb->failovers->getLatestEntry().vb_uuid;
}

uint64_t PassiveStreamEngineCtx::getHighSeqno() {
    RCPtr<VBucket> vb = engine->getVBucket(vbid);
    if (!vb) {
        return 0;
    }
    return vb->getHighSeqno();
}

const char* PassiveStreamEngineCtx::logHeader() {
    return consumer->logHeader();
}

void PassiveStreamEngineCtx::notify() {
    consumer->notifyStreamReady(vbid);
}
