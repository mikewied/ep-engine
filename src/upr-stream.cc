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

#include "ep_engine.h"
#include "kvstore.h"
#include "statwriter.h"
#include "upr-stream.h"
#include "upr-response.h"

const uint64_t Stream::uprMaxSeqno = std::numeric_limits<uint64_t>::max();

class CacheCallback : public Callback<CacheLookup> {
public:
    CacheCallback(EventuallyPersistentEngine* e, active_stream_t &s) :
        engine_(e), stream_(s) {}

    void callback(CacheLookup &lookup);

private:
    EventuallyPersistentEngine* engine_;
    active_stream_t stream_;
};

void CacheCallback::callback(CacheLookup &lookup) {
    RCPtr<VBucket> vb = engine_->getEpStore()->getVBucket(lookup.getVBucketId());
    int bucket_num(0);
    LockHolder lh = vb->ht.getLockedBucket(lookup.getKey(), &bucket_num);
    StoredValue *v = vb->ht.unlocked_find(lookup.getKey(), bucket_num);
    if (v && v->isResident() && v->getBySeqno() == lookup.getBySeqno()) {
        Item* it = v->toItem(false, lookup.getVBucketId());
        lh.unlock();
        stream_->backfillReceived(it);
        setStatus(ENGINE_KEY_EEXISTS);
    } else {
        setStatus(ENGINE_SUCCESS);
    }
}

class DiskCallback : public Callback<GetValue> {
public:
    DiskCallback(active_stream_t &s) :
        stream_(s) {}

    void callback(GetValue &val) {
        assert(val.getValue());
        stream_->backfillReceived(val.getValue());
    }

private:
    active_stream_t stream_;
};

class UprBackfill : public GlobalTask {
public:
    UprBackfill(EventuallyPersistentEngine* e, active_stream_t &s,
                uint64_t start_seqno, uint64_t end_seqno, const Priority &p,
                double sleeptime = 0, bool shutdown = false) :
        GlobalTask(e, p, sleeptime, shutdown), engine(e), stream(s),
        startSeqno(start_seqno), endSeqno(end_seqno) {

        KVStore* kvstore = engine->getEpStore()->getAuxUnderlying();
        numItems = kvstore->getNumItems(s->getVBucket(), startSeqno, endSeqno);
        stream->incrBackfillRemaining(numItems);
    }

    bool run();

    std::string getDescription();

private:
    EventuallyPersistentEngine *engine;
    active_stream_t             stream;
    uint64_t                    startSeqno;
    uint64_t                    endSeqno;
    size_t                      numItems;
};

bool UprBackfill::run() {
    KVStore* kvstore = engine->getEpStore()->getAuxUnderlying();

    if (numItems > 0) {
        shared_ptr<Callback<GetValue> >
            cb(new DiskCallback(stream));
        shared_ptr<Callback<CacheLookup> >
            cl(new CacheCallback(engine, stream));
        kvstore->dump(stream->getVBucket(), startSeqno, endSeqno, cb, cl);
    }

    stream->completeBackfill();

    return false;
}

std::string UprBackfill::getDescription() {
    std::stringstream ss;
    ss << "Upr backfill for vbucket " << stream->getVBucket();
    return ss.str();
}

Stream::Stream(std::string &name, uint32_t flags, uint32_t opaque, uint16_t vb,
               uint64_t start_seqno, uint64_t end_seqno, uint64_t vb_uuid,
               uint64_t high_seqno)
    : name_(name), flags_(flags), opaque_(opaque), vb_(vb),
      start_seqno_(start_seqno), end_seqno_(end_seqno), vb_uuid_(vb_uuid),
      high_seqno_(high_seqno), state_(STREAM_PENDING) {
}

void Stream::clear_UNLOCKED() {
    while (!readyQ.empty()) {
        UprResponse* resp = readyQ.front();
        delete resp;
        readyQ.pop();
    }
}

const char * Stream::stateName(stream_state_t st) const {
    static const char * const stateNames[] = {
        "pending", "backfilling", "in-memory", "takeover-send", "takeover-wait",
        "reading", "dead"
    };
    assert(st >= STREAM_PENDING && st <= STREAM_DEAD);
    return stateNames[st];
}

void Stream::addStats(ADD_STAT add_stat, const void *c) {
    const int bsize = 128;
    char buffer[bsize];
    snprintf(buffer, bsize, "%s:stream_%d_flags", name_.c_str(), vb_);
    add_casted_stat(buffer, flags_, add_stat, c);
    snprintf(buffer, bsize, "%s:stream_%d_opaque", name_.c_str(), vb_);
    add_casted_stat(buffer, opaque_, add_stat, c);
    snprintf(buffer, bsize, "%s:stream_%d_start_seqno", name_.c_str(), vb_);
    add_casted_stat(buffer, start_seqno_, add_stat, c);
    snprintf(buffer, bsize, "%s:stream_%d_end_seqno", name_.c_str(), vb_);
    add_casted_stat(buffer, end_seqno_, add_stat, c);
    snprintf(buffer, bsize, "%s:stream_%d_vb_uuid", name_.c_str(), vb_);
    add_casted_stat(buffer, vb_uuid_, add_stat, c);
    snprintf(buffer, bsize, "%s:stream_%d_high_seqno", name_.c_str(), vb_);
    add_casted_stat(buffer, high_seqno_, add_stat, c);
    snprintf(buffer, bsize, "%s:stream_%d_state", name_.c_str(), vb_);
    add_casted_stat(buffer, stateName(state_), add_stat, c);
}

ActiveStream::ActiveStream(EventuallyPersistentEngine* e, std::string &n,
                           uint32_t flags, uint32_t opaque, uint16_t vb,
                           uint64_t st_seqno, uint64_t en_seqno,
                           uint64_t vb_uuid, uint64_t hi_seqno)
    :  Stream(n, flags, opaque, vb, st_seqno, en_seqno, vb_uuid, hi_seqno),
       lastReadSeqno(st_seqno), lastSentSeqno(st_seqno), curChkSeqno(st_seqno),
       takeoverSeqno(0), takeoverState(vbucket_state_pending),
       backfillRemaining(0), engine(e),
       isBackfillTaskRunning(false) {

    if (flags_ & UPR_ADD_STREAM_FLAG_TAKEOVER) {
        end_seqno_ = uprMaxSeqno;
    }

    if (start_seqno_ > end_seqno_) {
        endStream(END_STREAM_OK);
    }
}

UprResponse* ActiveStream::next() {
    LockHolder lh(streamMutex);
    switch (state_) {
        case STREAM_PENDING:
            return NULL;
        case STREAM_BACKFILLING:
            return backfillPhase();
        case STREAM_IN_MEMORY:
            return inMemoryPhase();
        case STREAM_TAKEOVER_SEND:
            return takeoverSendPhase();
        case STREAM_TAKEOVER_WAIT:
            return takeoverWaitPhase();
        case STREAM_DEAD:
            return deadPhase();
        default:
            LOG(EXTENSION_LOG_WARNING, "Invalid state '%s' for vbucket %d",
                stateName(state_), vb_);
            abort();
    }
}

void ActiveStream::backfillReceived(Item* itm) {
    LockHolder lh(streamMutex);
    if (state_ == STREAM_BACKFILLING) {
        MutationResponse* m = new MutationResponse(itm, opaque_);
        readyQ.push(m);

        lastReadSeqno = itm->getBySeqno();
        itemsFromBackfill++;
    } else {
        delete itm;
    }
}

void ActiveStream::completeBackfill() {
    LockHolder lh(streamMutex);
    if (state_ == STREAM_BACKFILLING) {
        isBackfillTaskRunning = false;
        readyQ.push(new SnapshotMarker(opaque_, vb_));
        LOG(EXTENSION_LOG_WARNING, "Backfill complete for vb %d, last seqno "
            "read: %ld", vb_, lastReadSeqno);
        if (lastReadSeqno >= end_seqno_) {
            endStream(END_STREAM_OK);
        } else if (flags_ & UPR_ADD_STREAM_FLAG_TAKEOVER) {
            transitionState(STREAM_TAKEOVER_SEND);
        } else {
            transitionState(STREAM_IN_MEMORY);
        }
    }
}

void ActiveStream::setVBucketStateAckRecieved() {
    if (state_ == STREAM_TAKEOVER_WAIT) {
        if (takeoverState == vbucket_state_pending) {
            RCPtr<VBucket> vbucket = engine->getVBucket(vb_);
            engine->getEpStore()->setVBucketState(vb_, vbucket_state_dead, false);
            takeoverSeqno = vbucket->getHighSeqno();
            takeoverState = vbucket_state_active;
            transitionState(STREAM_TAKEOVER_SEND);
        } else {
            endStream(END_STREAM_OK);
        }
    } else {
        printf("Wrong state: %s\n", stateName(state_));
        LOG(EXTENSION_LOG_WARNING, "Unexpected ack for set vbucket op on vb %d"
            " stream '%s' state '%s'", vb_, name_.c_str(), stateName(state_));
    }
}

UprResponse* ActiveStream::backfillPhase() {
    UprResponse* resp = nextQueuedItem();

    if (resp && backfillRemaining > 0 &&
        (resp->getEvent() == UPR_MUTATION ||
         resp->getEvent() == UPR_DELETION ||
         resp->getEvent() == UPR_EXPIRATION)) {
        backfillRemaining--;
    }

    if (!isBackfillTaskRunning && readyQ.empty()) {
        backfillRemaining = 0;
        if (flags_ & UPR_ADD_STREAM_FLAG_TAKEOVER) {
            transitionState(STREAM_TAKEOVER_SEND);
        } else {
            transitionState(STREAM_IN_MEMORY);
        }
    }

    return resp;
}

UprResponse* ActiveStream::inMemoryPhase() {
    UprResponse* resp = nextQueuedItem();

    if (!resp) {
        resp = nextCheckpointItem();
        if (resp && lastSentSeqno >= end_seqno_) {
            readyQ.push(new SnapshotMarker(opaque_, vb_));
            endStream(END_STREAM_OK);
        }
    }
    return resp;
}

UprResponse* ActiveStream::takeoverSendPhase() {
    UprResponse* resp = nextQueuedItem();

    if (!resp) {
        resp = nextCheckpointItem();
        if (lastSentSeqno >= takeoverSeqno) {
            readyQ.push(new SnapshotMarker(opaque_, vb_));
            readyQ.push(new SetVBucketState(opaque_, vb_, takeoverState));
            transitionState(STREAM_TAKEOVER_WAIT);
        }
    }
    return resp;
}

UprResponse* ActiveStream::takeoverWaitPhase() {
    return nextQueuedItem();
}

UprResponse* ActiveStream::deadPhase() {
    return nextQueuedItem();
}

void ActiveStream::addStats(ADD_STAT add_stat, const void *c) {
    Stream::addStats(add_stat, c);

    const int bsize = 128;
    char buffer[bsize];
    snprintf(buffer, bsize, "%s:stream_%d_backfilled", name_.c_str(), vb_);
    add_casted_stat(buffer, itemsFromBackfill, add_stat, c);
    snprintf(buffer, bsize, "%s:stream_%d_memory", name_.c_str(), vb_);
    add_casted_stat(buffer, itemsFromMemory, add_stat, c);
}

void ActiveStream::addTakeoverStats(ADD_STAT add_stat, const void *cookie) {
    LockHolder lh(streamMutex);

    RCPtr<VBucket> vb = engine->getVBucket(vb_);
    add_casted_stat("name", name_, add_stat, cookie);
    if (!vb || state_ == STREAM_DEAD) {
        add_casted_stat("status", "completed", add_stat, cookie);
        add_casted_stat("estimate", 0, add_stat, cookie);
        return;
    }

    size_t total = backfillRemaining;
    if (state_ == STREAM_BACKFILLING) {
        add_casted_stat("status", "backfilling", add_stat, cookie);
    } else {
        add_casted_stat("status", "in-memory", add_stat, cookie);
    }
    add_casted_stat("backfillRemaining", backfillRemaining, add_stat, cookie);

    item_eviction_policy_t iep = engine->getEpStore()->getItemEvictionPolicy();
    size_t vb_items = vb->getNumItems(iep);
    size_t chk_items = vb_items > 0 ?
                vb->checkpointManager.getNumItemsForTAPConnection(name_) : 0;

    if (end_seqno_ < curChkSeqno) {
        chk_items = 0;
    } else if ((end_seqno_ - curChkSeqno) < chk_items) {
        chk_items = end_seqno_ - curChkSeqno + 1;
    }
    total += chk_items;

    add_casted_stat("estimate", total, add_stat, cookie);
    add_casted_stat("chk_items", chk_items, add_stat, cookie);
    add_casted_stat("vb_items", vb_items, add_stat, cookie);
}

UprResponse* ActiveStream::nextQueuedItem() {
    if (!readyQ.empty()) {
        UprResponse* response = readyQ.front();
        if (response->getEvent() == UPR_MUTATION ||
            response->getEvent() == UPR_DELETION ||
            response->getEvent() == UPR_EXPIRATION) {
            lastSentSeqno = dynamic_cast<MutationResponse*>(response)->getBySeqno();
        }
        readyQ.pop();
        return response;
    }
    return NULL;
}

UprResponse* ActiveStream::nextCheckpointItem() {
    RCPtr<VBucket> vbucket = engine->getVBucket(vb_);
    bool isLast;
    queued_item qi = vbucket->checkpointManager.nextItem(name_, isLast);

    if (qi->getOperation() == queue_op_set ||
        qi->getOperation() == queue_op_del) {
        lastReadSeqno = qi->getBySeqno();
        lastSentSeqno = qi->getBySeqno();
        curChkSeqno = qi->getBySeqno();
        itemsFromMemory++;
    }

    if (qi->getOperation() == queue_op_set ||
        qi->getOperation() == queue_op_del) {
        Item* itm = new Item(qi->getKey(), qi->getFlags(), qi->getExptime(),
                             qi->getValue(), qi->getCas(), qi->getBySeqno(),
                             qi->getVBucketId(), qi->getRevSeqno());
        if (qi->isDeleted()) {
            itm->setDeleted();
        }
        return new MutationResponse(itm, opaque_);
    } else if (qi->getOperation() == queue_op_checkpoint_end) {
        return new SnapshotMarker(opaque_, vb_);
    }
    return NULL;
}

void ActiveStream::endStream(end_stream_status_t reason) {
    readyQ.push(new StreamEndResponse(opaque_, reason, vb_));
    transitionState(STREAM_DEAD);
}

void ActiveStream::scheduleBackfill() {
    if (!isBackfillTaskRunning) {
        RCPtr<VBucket> vbucket = engine->getVBucket(vb_);
        curChkSeqno = vbucket->checkpointManager.registerTAPCursorBySeqno(name_, lastReadSeqno);

        if (lastReadSeqno < curChkSeqno) {
            uint64_t backfillEnd = end_seqno_;
            if (curChkSeqno < backfillEnd) {
                backfillEnd = curChkSeqno - 1;
            }
            LOG(EXTENSION_LOG_WARNING, "Scheduling backfill for vb %d (%d to %d)",
                vb_, lastReadSeqno, curChkSeqno);
            active_stream_t stream(this);
            ExTask task = new UprBackfill(engine, stream, lastReadSeqno, backfillEnd,
                                          Priority::TapBgFetcherPriority, 0, false);
            ExecutorPool::get()->schedule(task, AUXIO_TASK_IDX);
            isBackfillTaskRunning = true;
        }
    }
}

void ActiveStream::transitionState(stream_state_t newState) {
    LOG(EXTENSION_LOG_DEBUG, "Transitioning from %s to %s", stateName(state_),
        stateName(newState));

    if (state_ == newState) {
        return;
    }

    switch (state_) {
        case STREAM_PENDING:
            assert(newState == STREAM_BACKFILLING || newState == STREAM_DEAD);
            break;
        case STREAM_BACKFILLING:
            assert(newState == STREAM_IN_MEMORY ||
                   newState == STREAM_TAKEOVER_SEND ||
                   newState == STREAM_DEAD);
            break;
        case STREAM_IN_MEMORY:
            assert(newState == STREAM_BACKFILLING || newState == STREAM_DEAD);
            break;
        case STREAM_TAKEOVER_SEND:
            assert(newState == STREAM_TAKEOVER_WAIT || newState == STREAM_DEAD);
            break;
        case STREAM_TAKEOVER_WAIT:
            assert(newState == STREAM_TAKEOVER_SEND || newState == STREAM_DEAD);
            break;
        default:
            LOG(EXTENSION_LOG_WARNING, "Invalid Transition from %s to %s",
                stateName(state_), newState);
            abort();
    }

    switch (newState) {
        case STREAM_BACKFILLING:
            scheduleBackfill();
            break;
        case STREAM_TAKEOVER_SEND:
            takeoverSeqno = engine->getVBucket(vb_)->getHighSeqno();
            break;
        case STREAM_DEAD:
            engine->getVBucket(vb_)->checkpointManager.removeTAPCursor(name_);
            break;
        default:
            break;
    }

    state_ = newState;
}

PassiveStream::PassiveStream(std::string &name, uint32_t flags, uint32_t opaque,
                             uint16_t vb, uint64_t st_seqno, uint64_t en_seqno,
                             uint64_t vb_uuid, uint64_t hi_seqno)
    : Stream(name, flags, opaque, vb, st_seqno, en_seqno, vb_uuid, hi_seqno) {
    LockHolder lh(streamMutex);
    readyQ.push(new StreamRequest(vb, opaque, flags, st_seqno, en_seqno,
                                  vb_uuid, hi_seqno));
}

void PassiveStream::acceptStream(uint16_t status, uint32_t add_opaque) {
    LockHolder lh(streamMutex);
    if (state_ == STREAM_PENDING) {
        if (status == ENGINE_SUCCESS) {
            transitionState(STREAM_READING);
        } else {
            transitionState(STREAM_DEAD);
        }
        readyQ.push(new AddStreamResponse(add_opaque, opaque_, status));
    }
}

void PassiveStream::reconnectStream(RCPtr<VBucket> &vb,
                                    uint32_t new_opaque,
                                    uint64_t start_seqno) {
    vb_uuid_ = vb->failovers->getLatestEntry().vb_uuid;
    high_seqno_ = vb->failovers->getLatestEntry().by_seqno;
    LockHolder lh(streamMutex);
    readyQ.push(new StreamRequest(vb_, new_opaque, flags_, start_seqno,
                                  end_seqno_, vb_uuid_, high_seqno_));
}

UprResponse* PassiveStream::next() {
    LockHolder lh(streamMutex);
    if (!readyQ.empty()) {
        UprResponse* resp = readyQ.front();
        readyQ.pop();
        return resp;
    }
    return NULL;
}

void PassiveStream::transitionState(stream_state_t newState) {
    LOG(EXTENSION_LOG_DEBUG, "Transitioning from %s to %s", stateName(state_),
        stateName(newState));

    if (state_ == newState) {
        return;
    }

    switch (state_) {
        case STREAM_PENDING:
            assert(newState == STREAM_READING || newState == STREAM_DEAD);
            break;
        case STREAM_READING:
            assert(newState == STREAM_PENDING || newState == STREAM_DEAD);
            break;
        default:
            LOG(EXTENSION_LOG_WARNING, "Invalid Transition from %s to %s",
                stateName(state_), newState);
            abort();
    }

    state_ = newState;
}
