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

#ifndef SRC_CHECKPOINT_H_
#define SRC_CHECKPOINT_H_ 1

#include "config.h"

#include <set>

#include "item.h"

class Checkpoint;
class CheckpointCursor;
class CheckpointManager;

/**
 * The state of a given checkpoint.
 */
typedef enum {
    CHECKPOINT_OPEN, //!< The checkpoint is open.
    CHECKPOINT_CLOSED  //!< The checkpoint is not open.
} checkpoint_state;

/**
 * The checkpoint index maps a key to a checkpoint index_entry.
 */
typedef unordered_map<std::string, std::list<queued_item>::iterator> checkpoint_index;

/**
 * The cursor index maps checkpoint cursor names to checkpoint cursors
 */
typedef std::map<const std::string, CheckpointCursor> cursor_index;

/**
 * Result from invoking queueDirty in the current open checkpoint.
 */
typedef enum {
    /*
     * The item exists on the right hand side of the persistence cursor. The
     * item will be deduplicated and doesn't change the size of the checkpoint.
     */
    EXISTING_ITEM,

    /**
     * The item exists on the left hand side of the persistence cursor. It will
     * be dedeuplicated and moved the to right hand side, but the item needs
     * to be re-persisted.
     */
    PERSIST_AGAIN,

    /**
     * The item doesn't exist yet in the checkpoint. Adding this item will
     * increase the size of the checkpoint.
     */
    NEW_ITEM
} queue_dirty_t;

/**
 * A checkpoint cursor
 */
class CheckpointCursor {
    friend class CheckpointManager;
    friend class Checkpoint;
public:

    CheckpointCursor() { }

    CheckpointCursor(const std::string &n)
        : name(n),
          currentCheckpoint(),
          currentPos(),
          offset(0),
          fromBeginningOnChkCollapse(false) { }

    CheckpointCursor(const std::string &n,
                     std::list<Checkpoint*>::iterator checkpoint,
                     std::list<queued_item>::iterator pos,
                     size_t os,
                     bool beginningOnChkCollapse) :
        name(n), currentCheckpoint(checkpoint), currentPos(pos),
        offset(os), fromBeginningOnChkCollapse(beginningOnChkCollapse) { }

    // We need to define the copy construct explicitly due to the fact
    // that std::atomic implicitly deleted the assignment operator
    CheckpointCursor(const CheckpointCursor &other) :
        name(other.name), currentCheckpoint(other.currentCheckpoint),
        currentPos(other.currentPos), offset(other.offset.load()),
        fromBeginningOnChkCollapse(other.fromBeginningOnChkCollapse) { }

    CheckpointCursor &operator=(const CheckpointCursor &other) {
        name.assign(other.name);
        currentCheckpoint = other.currentCheckpoint;
        currentPos = other.currentPos;
        offset.store(other.offset.load());
        fromBeginningOnChkCollapse = other.fromBeginningOnChkCollapse;
        return *this;
    }

    void decrOffset(size_t offset);

    void decrPosition();

    static const std::string pCursorName;

private:
    std::string                      name;
    std::list<Checkpoint*>::iterator currentCheckpoint;
    std::list<queued_item>::iterator currentPos;
    AtomicValue<size_t>              offset;
    bool                             fromBeginningOnChkCollapse;
};

/**
 * Representation of a checkpoint used in the unified queue for persistence and
 * replication.
 */
class Checkpoint {
public:
    Checkpoint(EPStats &st, uint64_t id, uint16_t vbid,
               checkpoint_state state = CHECKPOINT_OPEN) :
        stats(st), checkpointId(id), vbucketId(vbid), creationTime(ep_real_time()),
        checkpointState(state), numItems(0), memOverhead(0) {
        stats.memOverhead.fetch_add(memorySize());
        cb_assert(stats.memOverhead.load() < GIGANTOR);
    }

    ~Checkpoint();

    /**
     * Return the checkpoint Id
     */
    uint64_t getId() const {
        return checkpointId;
    }

    /**
     * Set the checkpoint Id
     * @param id the checkpoint Id to be set.
     */
    void setId(uint64_t id) {
        checkpointId = id;
    }

    /**
     * Return the creation timestamp of this checkpoint in sec.
     */
    rel_time_t getCreationTime() {
        return creationTime;
    }

    /**
     * Return the number of items belonging to this checkpoint.
     */
    size_t getNumItems() const {
        return numItems;
    }

    /**
     * Return the current state of this checkpoint.
     */
    checkpoint_state getState() const {
        return checkpointState;
    }

    /**
     * Set the current state of this checkpoint.
     * @param state the checkpoint's new state
     */
    void setState(checkpoint_state state);

    void popBackCheckpointEndItem();

    /**
     * Return the number of cursors that are currently walking through this checkpoint.
     */
    size_t getNumberOfCursors() const {
        return cursors.size();
    }

    /**
     * Register a cursor's name to this checkpoint
     */
    void registerCursorName(const std::string &name) {
        cursors.insert(name);
    }

    /**
     * Remove a cursor's name from this checkpoint
     */
    void removeCursorName(const std::string &name) {
        cursors.erase(name);
    }

    /**
     * Return true if the cursor with a given name exists in this checkpoint
     */
    bool hasCursorName(const std::string &name) const {
        return cursors.find(name) != cursors.end();
    }

    /**
     * Return the list of all cursor names in this checkpoint
     */
    const std::set<std::string> &getCursorNameList() const {
        return cursors;
    }

    /**
     * Queue an item to be written to persistent layer.
     * @param item the item to be persisted
     * @param checkpointManager the checkpoint manager to which this checkpoint belongs
     * @param bySeqno the by sequence number assigned to this mutation
     * @return a result indicating the status of the operation.
     */
    queue_dirty_t queueDirty(const queued_item &qi, CheckpointManager* manager);

    uint64_t getLowSeqno() {
        std::list<queued_item>::iterator pos = toWrite.begin();
        pos++;
        return (*pos)->getBySeqno();
    }

    uint64_t getHighSeqno() {
        std::list<queued_item>::reverse_iterator pos = toWrite.rbegin();
        return (*pos)->getBySeqno();
    }

    std::list<queued_item>::iterator begin() {
        return toWrite.begin();
    }

    std::list<queued_item>::iterator end() {
        return toWrite.end();
    }

    std::list<queued_item>::reverse_iterator rbegin() {
        return toWrite.rbegin();
    }

    std::list<queued_item>::reverse_iterator rend() {
        return toWrite.rend();
    }

    bool keyExists(const std::string &key);

    /**
     * Return the memory overhead of this checkpoint instance, except for the memory used by
     * all the items belonging to this checkpoint. The memory overhead of those items is
     * accounted separately in "ep_kv_size" stat.
     * @return memory overhead of this checkpoint instance.
     */
    size_t memorySize() {
        return sizeof(Checkpoint) + memOverhead;
    }

    /**
     * Merge the previous checkpoint into the this checkpoint by adding the items from
     * the previous checkpoint, which don't exist in this checkpoint.
     * @param pPrevCheckpoint pointer to the previous checkpoint.
     * @return the number of items added from the previous checkpoint.
     */
    size_t mergePrevCheckpoint(Checkpoint *pPrevCheckpoint);

    /**
     * Get the mutation id for a given key in this checkpoint
     * @param key a key to retrieve its mutation id
     * @return the mutation id for a given key
     */
    uint64_t getMutationIdForKey(const std::string &key);

private:
    EPStats                       &stats;
    uint64_t                       checkpointId;
    uint16_t                       vbucketId;
    rel_time_t                     creationTime;
    checkpoint_state               checkpointState;
    size_t                         numItems;
    std::set<std::string>          cursors; // List of cursors with their unique names.
    // List is used for queueing mutations as vector incurs shift operations for deduplication.
    std::list<queued_item>         toWrite;
    checkpoint_index               keyIndex;
    size_t                         memOverhead;
};

#endif  // SRC_CHECKPOINT_H_
