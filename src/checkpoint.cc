/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2011 Couchbase, Inc
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

#include "checkpoint.h"
#include "checkpoint_manager.h"

const std::string CheckpointCursor::pCursorName("persistence");

void CheckpointCursor::decrOffset(size_t decr) {
    if (offset >= decr) {
        offset.fetch_sub(decr);
    } else {
        offset = 0;
        LOG(EXTENSION_LOG_INFO, "%s cursor offset is negative. Reset it to 0.",
            name.c_str());
    }
}

void CheckpointCursor::decrPosition() {
    if (currentPos != (*currentCheckpoint)->begin()) {
        --currentPos;
    }
}

Checkpoint::~Checkpoint() {
    LOG(EXTENSION_LOG_INFO,
        "Checkpoint %llu for vbucket %d is purged from memory",
        checkpointId, vbucketId);
    stats.memOverhead.fetch_sub(memorySize());
    cb_assert(stats.memOverhead.load() < GIGANTOR);
}

void Checkpoint::setState(checkpoint_state state) {
    checkpointState = state;
}

void Checkpoint::popBackCheckpointEndItem() {
    if (!toWrite.empty() &&
        toWrite.back()->getOperation() == queue_op_checkpoint_end) {
        keyIndex.erase(toWrite.back()->getKey());
        toWrite.pop_back();
    }
}

bool Checkpoint::keyExists(const std::string &key) {
    return keyIndex.find(key) != keyIndex.end();
}

queue_dirty_t Checkpoint::queueDirty(const queued_item &qi,
                                     CheckpointManager* manager) {
    assert (checkpointState == CHECKPOINT_OPEN);
    queue_dirty_t rv;

    checkpoint_index::iterator it = keyIndex.find(qi->getKey());
    // Check if this checkpoint already had an item for the same key.
    if (it != keyIndex.end()) {
        rv = EXISTING_ITEM;
        std::list<queued_item>::iterator currPos = it->second;
        uint64_t currBySeqno = (*currPos)->getBySeqno();

        cursor_index::iterator map_it = manager->tapCursors.begin();
        for (; map_it != manager->tapCursors.end(); ++map_it) {

            if (*(map_it->second.currentCheckpoint) == this) {
                queued_item &tqi = *(map_it->second.currentPos);
                const std::string &key = tqi->getKey();
                checkpoint_index::iterator ita = keyIndex.find(key);
                if (ita != keyIndex.end()) {
                    uint64_t bySeqno = (*ita->second)->getBySeqno();
                    if (currBySeqno <= bySeqno &&
                        tqi->getOperation() != queue_op_checkpoint_start) {
                        map_it->second.decrOffset(1);
                        std::string& name = map_it->second.name;
                        if (name.compare(CheckpointCursor::pCursorName) == 0) {
                            rv = PERSIST_AGAIN;
                        }
                    }
                }
                // If an TAP cursor points to the existing item for the same
                // key, shift it left by 1
                if (map_it->second.currentPos == currPos) {
                    map_it->second.decrPosition();
                }
            }
        }

        toWrite.push_back(qi);
        // Remove the existing item for the same key from the list.
        toWrite.erase(currPos);
    } else {
        if (qi->getOperation() == queue_op_set ||
            qi->getOperation() == queue_op_del) {
            ++numItems;
        }
        rv = NEW_ITEM;
        // Push the new item into the list
        toWrite.push_back(qi);
    }

    if (qi->getNKey() > 0) {
        std::list<queued_item>::iterator last = toWrite.end();
        // Set the index of the key to the new item that is pushed back into
        // the list.
        keyIndex[qi->getKey()] = --last;
        if (rv == NEW_ITEM) {
            size_t newEntrySize = qi->getNKey() +
                                  sizeof(std::list<queued_item>::iterator) +
                                  sizeof(queued_item);
            memOverhead += newEntrySize;
            stats.memOverhead.fetch_add(newEntrySize);
            cb_assert(stats.memOverhead.load() < GIGANTOR);
        }
    }
    return rv;
}

size_t Checkpoint::mergePrevCheckpoint(Checkpoint *pPrevCheckpoint) {
    size_t numNewItems = 0;
    size_t newEntryMemOverhead = 0;
    std::list<queued_item>::reverse_iterator rit = pPrevCheckpoint->rbegin();

    LOG(EXTENSION_LOG_INFO,
        "Collapse the checkpoint %llu into the checkpoint %llu for vbucket %d",
        pPrevCheckpoint->getId(), checkpointId, vbucketId);

    std::list<queued_item>::iterator itr = toWrite.begin();
    uint64_t seqno = pPrevCheckpoint->getMutationIdForKey("dummy_key");
    (*itr)->setBySeqno(seqno);

    seqno = pPrevCheckpoint->getMutationIdForKey("checkpoint_start");
    ++itr;
    (*itr)->setBySeqno(seqno);

    for (; rit != pPrevCheckpoint->rend(); ++rit) {
        const std::string &key = (*rit)->getKey();
        if ((*rit)->getOperation() != queue_op_del &&
            (*rit)->getOperation() != queue_op_set) {
            continue;
        }
        checkpoint_index::iterator it = keyIndex.find(key);
        if (it == keyIndex.end()) {
            std::list<queued_item>::iterator pos = toWrite.begin();
            // Skip the first two meta items
            ++pos; ++pos;
            toWrite.insert(pos, *rit);
            keyIndex[key] = --pos;
            newEntryMemOverhead += key.size() +
                                   sizeof(std::list<queued_item>::iterator);
            ++numItems;
            ++numNewItems;
        }
    }
    memOverhead += newEntryMemOverhead;
    stats.memOverhead.fetch_add(newEntryMemOverhead);
    cb_assert(stats.memOverhead.load() < GIGANTOR);
    return numNewItems;
}

uint64_t Checkpoint::getMutationIdForKey(const std::string &key) {
    uint64_t mid = 0;
    checkpoint_index::iterator it = keyIndex.find(key);
    if (it != keyIndex.end()) {
        mid = (*it->second)->getBySeqno();
    }
    return mid;
}
