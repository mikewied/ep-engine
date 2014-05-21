/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2014 Couchbase, Inc
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

#include "locks.h"
#include "upr-stream.h"
#include "upr-response.h"

extern "C" {
    static rel_time_t basic_current_time(void) {
        return 0;
    }

    rel_time_t (*ep_current_time)() = basic_current_time;

    time_t ep_real_time() {
        return time(NULL);
    }

    static void backfill_thread(void* arg);

    static void stream_exe_thread(void* arg);
}

static bool debug = false;
static SyncObject syncvar;
static std::list<UprResponse*> results;

typedef enum {
    disk,
    memory,
    both
} insert_t;

class ActiveStreamTestCtx;

class BackfillParams {
public:
    BackfillParams(ActiveStreamTestCtx* c, ActiveStream* s, uint64_t st,
                   uint64_t en)
        : ctx(c), stream(s), start(st), end(en) {}

    ActiveStreamTestCtx* ctx;
    ActiveStream* stream;
    uint64_t start;
    uint64_t end;
};

class ActiveStreamTestCtx : public ActiveStreamCtx {
public:
    ActiveStreamTestCtx()
        : ActiveStreamCtx(), highSeqno(0), persistedSeqno(0) {}

    void scheduleBackfill(ActiveStream* stream, uint64_t start, uint64_t end) {
        BackfillParams* params = new BackfillParams(this, stream, start, end);
        cb_assert(cb_create_thread(&backfill, backfill_thread, params, 0) == 0);
    }

    void runBackfill(ActiveStream* stream, uint64_t start, uint64_t end) {
        if (diskQ.empty()) {
            stream->markDiskSnapshot(start, end);
        } else {
            stream->markDiskSnapshot(start, diskQ.back()->getBySeqno());
        }

        while (!diskQ.empty()) {
            Item* itm = diskQ.front();
            diskQ.pop();
            stream->backfillReceived(itm);
        }
        stream->completeBackfill();
    }

    void setVBucketState() {
        // Not implemented
    }

    queued_item getItemFromMemory(const std::string& name, uint64_t& snapEnd) {
        LockHolder lh(itemMutex);
        if (!memoryQ.empty()) {
            snapEnd = memoryQ.back()->getBySeqno();
            queued_item qi = memoryQ.front();
            memoryQ.pop_front();
            return qi;
        }
        return new Item(std::string(""), 0xffff, queue_op_empty, 0, 0);
    }

    uint64_t registerMemoryCursor(uint64_t regSeqno, uint64_t endSeqno,
                                  const std::string& name) {
        LockHolder lh(itemMutex);
        cb_assert(regSeqno <= highSeqno);

        if (memoryQ.empty()) {
            return persistedSeqno;
        } else if ((uint64_t)memoryQ.front()->getBySeqno() >= regSeqno) {
            return memoryQ.front()->getBySeqno();
        } else {
            uint64_t removeUpToSeqno;
            std::list<queued_item>::iterator itr = memoryQ.begin();
            for (; itr != memoryQ.end(); ++itr) {
                if ((uint64_t)(*itr)->getBySeqno() == regSeqno) {
                    removeUpToSeqno = (*itr)->getBySeqno();
                    break;
                } else if ((uint64_t)(*itr)->getBySeqno() > regSeqno) {
                    removeUpToSeqno = (*itr)->getBySeqno() - 1;
                    break;
                }
            }

            while (!memoryQ.empty() &&
                   (uint64_t)memoryQ.front()->getBySeqno() <= removeUpToSeqno) {
                memoryQ.pop_front();
            }

            if (memoryQ.empty()) {
                return highSeqno;
            }

            return memoryQ.front()->getBySeqno();
        }
        return 0;
    }

    void removeMemoryCursor(const std::string &name) {
        // Empty
    }

    uint64_t getHighSeqno() {
        return highSeqno;
    }

    uint64_t getNumVBucketItems() {
        // Not implemented
        return 0;
    }

    uint64_t getNumCheckpointItems(const std::string& name) {
        return memoryQ.size();
    }

    const char* logHeader() {
        return "test log header -";
    }

    void notify(bool schedule) {
        syncvar.notify();
    }

    void insertMutation(std::string& key, value_t& value, uint64_t bySeqno,
                        insert_t type) {
        LockHolder lh(itemMutex);
        cb_assert(highSeqno < bySeqno);

        highSeqno = bySeqno;
        if (type == memory || type == both) {
            memoryQ.push_back(new Item(key, 0, 0, value, 0, bySeqno));
        }
        if (type == disk || type == both) {
            diskQ.push(new Item(key, 0, 0, value, 0, bySeqno));
            persistedSeqno = bySeqno;
        }
    }

private:
    std::list<queued_item> memoryQ;
    std::queue<Item*> diskQ;

    Mutex itemMutex;
    cb_thread_t backfill;

    uint64_t highSeqno;
    uint64_t persistedSeqno;
};

static void backfill_thread(void* arg) {
    BackfillParams* params = static_cast<BackfillParams*>(arg);
    params->ctx->runBackfill(params->stream, params->start, params->end);
    delete params;
}

static void stream_exe_thread(void* arg) {
    ActiveStream* stream = static_cast<ActiveStream*>(arg);
    stream->setActive();

    UprResponse* resp = NULL;
    while (!resp || resp->getEvent() != UPR_STREAM_END) {
        LockHolder lh(syncvar);
        resp = stream->next();

        if (!resp) {
            syncvar.wait();
            continue;
        }
        lh.unlock();

        results.push_back(resp);

        if (debug) {
            if (resp->getEvent() == UPR_MUTATION ) {
                MutationResponse* m = static_cast<MutationResponse*>(resp);
                printf("Mutation received with key '%s' and seqno %llu\n",
                       m->getItem()->getKey().c_str(), m->getBySeqno());
            }
            else if (resp->getEvent() == UPR_DELETION) {
                MutationResponse* d = static_cast<MutationResponse*>(resp);
                printf("Deletion received with key '%s' and seqno %llu\n",
                       d->getItem()->getKey().c_str(), d->getBySeqno());
            } else if (resp->getEvent() == UPR_SNAPSHOT_MARKER) {
                SnapshotMarker* sm = static_cast<SnapshotMarker*>(resp);
                printf("Marker received with range %llu to %llu from %s\n",
                       sm->getStartSeqno(), sm->getEndSeqno(),
                       sm->getFlags() ? "disk" : "memory");
            } else if (resp->getEvent() == UPR_STREAM_END) {
                StreamEndResponse* se = static_cast<StreamEndResponse*>(resp);
                printf("Recived stream end with reason %d\n", se->getFlags());
            }
        }
    }
}

static void verify_stream_correctness() {
    if (results.empty()) {
        return;
    }

    // All streams should begin with a snapshot marker
    cb_assert(results.front()->getEvent() == UPR_SNAPSHOT_MARKER);

    // All streams should end with a stream end message
    cb_assert(results.back()->getEvent() == UPR_STREAM_END);

    std::list<UprResponse*>::iterator itr = results.begin();

    uint64_t snaphot_start_seqno = 0;
    uint64_t snaphot_end_seqno = 0;
    uint64_t last_by_seqno = 0;
    for (; itr != results.end(); ++itr) {
        if ((*itr)->getEvent() == UPR_MUTATION ||
            (*itr)->getEvent() == UPR_DELETION ||
            (*itr)->getEvent() == UPR_EXPIRATION) {
            MutationResponse* m = static_cast<MutationResponse*>(*itr);

            // Make sure the by seqno is always increasing
            cb_assert(last_by_seqno < m->getBySeqno());

            // Make sure the by seqno is valid in respect to the last marker
            cb_assert(snaphot_start_seqno <= m->getBySeqno());
            cb_assert(snaphot_end_seqno >= m->getBySeqno());

            last_by_seqno = m->getBySeqno();
        } else if ((*itr)->getEvent() == UPR_SNAPSHOT_MARKER) {
            SnapshotMarker* sm = static_cast<SnapshotMarker*>(*itr);
            snaphot_start_seqno = sm->getStartSeqno();
            snaphot_end_seqno = sm->getEndSeqno();
        }
    }
}

static void cleanup() {
    while (!results.empty()) {
        UprResponse* response = results.front();
        results.pop_front();
        if (response->getEvent() == UPR_MUTATION) {
            MutationResponse* m = static_cast<MutationResponse*>(response);
            delete m->getItem();
        }
        delete response;
    }
}

static void test_basic_memory_stream() {
    ActiveStreamTestCtx* ctx = new ActiveStreamTestCtx();

    for (int i = 1; i <= 10; i++) {
        std::stringstream ss;
        ss << "key" << i;

        std::string key(ss.str());
        value_t val(Blob::New("value", NULL, 0));
        ctx->insertMutation(key, val, i, memory);
    }

    uint64_t endSeqno = ctx->getHighSeqno();
    ActiveStream stream(ctx, "stream", 0, 0, 0, 0, endSeqno, 0, 0, 0);

    cb_thread_t thread;
    cb_assert(cb_create_thread(&thread, stream_exe_thread, &stream, 0) == 0);
    cb_join_thread(thread);

    verify_stream_correctness();
    cleanup();
}

static void test_partial_memory_stream() {
    ActiveStreamTestCtx* ctx = new ActiveStreamTestCtx();

    for (int i = 1; i <= 10; i++) {
        std::stringstream ss;
        ss << "key" << i;

        std::string key(ss.str());
        value_t val(Blob::New("value", NULL, 0));
        ctx->insertMutation(key, val, i, memory);
    }

    uint64_t endSeqno = ctx->getHighSeqno();
    ActiveStream stream(ctx, "stream", 0, 0, 0, 5, endSeqno, 0, 5, 5);

    cb_thread_t thread;
    cb_assert(cb_create_thread(&thread, stream_exe_thread, &stream, 0) == 0);
    cb_join_thread(thread);

    verify_stream_correctness();
    cleanup();
}

static void test_basic_disk_stream() {
    ActiveStreamTestCtx* ctx = new ActiveStreamTestCtx();

    for (int i = 1; i <= 10; i++) {
        std::stringstream ss;
        ss << "key" << i;

        std::string key(ss.str());
        value_t val(Blob::New("value", NULL, 0));
        ctx->insertMutation(key, val, i, disk);
    }

    uint64_t endSeqno = ctx->getHighSeqno();
    ActiveStream stream(ctx, "stream", 0, 0, 0, 0, endSeqno, 0, 0, 0);

    cb_thread_t thread;
    cb_assert(cb_create_thread(&thread, stream_exe_thread, &stream, 0) == 0);
    cb_join_thread(thread);

    verify_stream_correctness();
    cleanup();
}

static void test_basic_full_stream() {
    ActiveStreamTestCtx* ctx = new ActiveStreamTestCtx();

    for (int i = 1; i <= 10; i++) {
        std::stringstream ss;
        ss << "key" << i;

        std::string key(ss.str());
        value_t val(Blob::New("value", NULL, 0));
        if (i > 5) {
            ctx->insertMutation(key, val, i, memory);
        } else {
            ctx->insertMutation(key, val, i, disk);
        }
    }

    uint64_t endSeqno = ctx->getHighSeqno();
    ActiveStream stream(ctx, "stream", 0, 0, 0, 0, endSeqno, 0, 0, 0);

    cb_thread_t thread;
    cb_assert(cb_create_thread(&thread, stream_exe_thread, &stream, 0) == 0);
    cb_join_thread(thread);

    verify_stream_correctness();
    cleanup();
}

static void test_basic_full_stream_with_overlap() {
    ActiveStreamTestCtx* ctx = new ActiveStreamTestCtx();

    for (int i = 1; i <= 10; i++) {
        std::stringstream ss;
        ss << "key" << i;

        std::string key(ss.str());
        value_t val(Blob::New("value", NULL, 0));
        if (i < 3) {
            ctx->insertMutation(key, val, i, disk);
        } else if (i >= 3 && i < 7) {
            ctx->insertMutation(key, val, i, both);
        } else {
            ctx->insertMutation(key, val, i, memory);
        }
    }

    uint64_t endSeqno = ctx->getHighSeqno();
    ActiveStream stream(ctx, "stream", 0, 0, 0, 0, endSeqno, 0, 0, 0);

    cb_thread_t thread;
    cb_assert(cb_create_thread(&thread, stream_exe_thread, &stream, 0) == 0);
    cb_join_thread(thread);

    verify_stream_correctness();
    cleanup();
}

int main(int argc, char **argv) {
    (void)argc;
    (void)argv;

    putenv(strdup("ALLOW_NO_STATS_UPDATE=yeah"));

    test_basic_memory_stream();
    test_partial_memory_stream();
    test_basic_disk_stream();
    test_basic_full_stream();
    test_basic_full_stream_with_overlap();

    return 0;
}
