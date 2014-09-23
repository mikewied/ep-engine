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

#ifndef SRC_CHECKPOINT_MANAGER_H_
#define SRC_CHECKPOINT_MANAGER_H_ 1

#include "config.h"

#include <list>
#include <map>
#include <set>
#include <string>
#include <vector>

#include "atomic.h"
#include "checkpoint.h"
#include "common.h"
#include "item.h"
#include "locks.h"
#include "stats.h"

#define MIN_CHECKPOINT_ITEMS 10
#define MAX_CHECKPOINT_ITEMS 50000
#define DEFAULT_CHECKPOINT_ITEMS 500

#define MIN_CHECKPOINT_PERIOD 1 //  1 sec.
#define MAX_CHECKPOINT_PERIOD 3600 // 3600 sec.
#define DEFAULT_CHECKPOINT_PERIOD 5 // 5 sec.

#define DEFAULT_MAX_CHECKPOINTS 2
#define MAX_CHECKPOINTS_UPPER_BOUND 5

class CheckpointConfig;
class VBucket;

typedef std::pair<uint64_t, bool> CursorRegResult;

/**
 * Representation of a checkpoint manager that maintains the list of checkpoints
 * for each vbucket.
 */
class CheckpointManager {
    friend class Checkpoint;
    friend class EventuallyPersistentEngine;
    friend class Consumer;
    friend class TapConsumer;
public:

    CheckpointManager(EPStats &st, uint16_t vbucket, CheckpointConfig &config,
                      int64_t lastSeqno, uint64_t checkpointId = 1) :
        stats(st), checkpointConfig(config), vbucketId(vbucket), numItems(0),
        lastBySeqno(lastSeqno), lastClosedChkBySeqno(lastSeqno),
        isCollapsedCheckpoint(false),
        pCursorPreCheckpointId(0) {
        addNewCheckpoint(checkpointId);
        registerCursor("persistence", checkpointId);
    }

    ~CheckpointManager();

    uint64_t getOpenCheckpointId_UNLOCKED();
    uint64_t getOpenCheckpointId();

    uint64_t getLastClosedCheckpointId_UNLOCKED();
    uint64_t getLastClosedCheckpointId();

    void setOpenCheckpointId_UNLOCKED(uint64_t id);

    void setOpenCheckpointId(uint64_t id) {
        LockHolder lh(queueLock);
        setOpenCheckpointId_UNLOCKED(id);
    }

    /**
     * Remove closed unreferenced checkpoints and return them through the vector.
     * @param vbucket the vbucket that this checkpoint manager belongs to.
     * @param newOpenCheckpointCreated the flag indicating if the new open checkpoint was created
     * as a result of running this function.
     * @return the number of items that are purged from checkpoint
     */
    size_t removeClosedUnrefCheckpoints(const RCPtr<VBucket> &vbucket,
                                        bool &newOpenCheckpointCreated);

    /**
     * Register the cursor for getting items whose bySeqno values are between
     * startBySeqno and endBySeqno, and close the open checkpoint if endBySeqno
     * belongs to the open checkpoint.
     * @param startBySeqno start bySeqno.
     * @return Cursor registration result which consists of (1) the bySeqno with
     * which the cursor can start and (2) flag indicating if the cursor starts
     * with the first item on a checkpoint.
     */
    CursorRegResult registerCursorBySeqno(const std::string &name,
                                          uint64_t startBySeqno);

    /**
     * Register the new cursor for a given connection
     * @param name the name of a given connection
     * @param checkpointId the checkpoint Id to start with.
     * @param alwaysFromBeginning the flag indicating if a cursor should be set to the beginning of
     * checkpoint to start with, even if the cursor is currently in that checkpoint.
     * @return true if the checkpoint to start with exists in the queue.
     */
    bool registerCursor(const std::string &name, uint64_t checkpointId = 1,
                        bool alwaysFromBeginning = false);

    /**
     * Remove the cursor for a given connection.
     * @param name the name of a given connection
     * @return true if the cursor is removed successfully.
     */
    bool removeCursor(const std::string &name);

    /**
     * Get the Id of the checkpoint where the given connections cursor is currently located.
     * If the cursor is not found, return 0 as a checkpoint Id.
     * @param name the name of a given connection
     * @return the checkpoint Id for a given connections cursor.
     */
    uint64_t getCheckpointIdForCursor(const std::string &name);

    size_t getNumOfCursors();

    std::list<std::string> getCursorNames();

    /**
     * Queue an item to be written to persistent layer.
     * @param item the item to be persisted.
     * @param vbucket the vbucket that a new item is pushed into.
     * @param bySeqno the sequence number assigned to this mutation
     * @return true if an item queued increases the size of persistence queue by 1.
     */
    bool queueDirty(const RCPtr<VBucket> &vb, queued_item& qi, bool genSeqno);

    /**
     * Return the next item to be sent to a given connection
     * @param name the name of a given connection
     * @param isLastMutationItem flag indicating if the item to be returned is
     * the last mutation one in the closed checkpoint.
     * @return the next item to be sent to a given connection.
     */
    queued_item nextItem(const std::string &name, bool &isLastMutationItem);

    void getAllItemsForCursor(const std::string& name,
                              std::vector<queued_item> &items);

    /**
     * Return the total number of items that belong to this checkpoint manager.
     */
    size_t getNumItems() {
        return numItems;
    }

    size_t getNumOpenChkItems();

    size_t getNumCheckpoints();

    size_t getNumItemsForCursor(const std::string &name);

    /**
     * Clear all the checkpoints managed by this checkpoint manager.
     */
    void clear(vbucket_state_t vbState);

    /**
     * If a given cursor currently points to the checkpoint_end dummy item,
     * decrease its current position by 1. This function is mainly used for
     * checkpoint synchronization between the master and slave nodes.
     * @param name the name of a given connection
     */
    void decrCursorFromCheckpointEnd(const std::string &name);

    bool hasNext(const std::string &name);

    const CheckpointConfig &getCheckpointConfig() const {
        return checkpointConfig;
    }

    void addStats(ADD_STAT add_stat, const void *cookie);

    /**
     * Create a new open checkpoint by force.
     * @return the new open checkpoint id
     */
    uint64_t createNewCheckpoint();

    void resetCursors(const std::list<std::string> &cursors);

    /**
     * Get id of the previous checkpoint that is followed by the checkpoint
     * where the persistence cursor is currently walking.
     */
    uint64_t getPersistenceCursorPreChkId();

    /**
     * Update the checkpoint manager persistence cursor checkpoint offset
     */
    void itemsPersisted();

    /**
     * This method performs the following steps for creating a new checkpoint with a given ID i1:
     * 1) Check if the checkpoint manager contains any checkpoints with IDs >= i1.
     * 2) If exists, collapse all checkpoints and set the open checkpoint id to a given ID.
     * 3) Otherwise, simply create a new open checkpoint with a given ID.
     * This method is mainly for dealing with rollback events from a producer.
     * @param id the id of a checkpoint to be created.
     * @param vbucket vbucket of the checkpoint.
     */
    void checkAndAddNewCheckpoint(uint64_t id, const RCPtr<VBucket> &vbucket);

    bool incrCursor(CheckpointCursor &cursor);

    void setBySeqno(int64_t seqno) {
        LockHolder lh(queueLock);
        lastBySeqno = seqno;
    }

    int64_t getHighSeqno() {
        LockHolder lh(queueLock);
        return lastBySeqno;
    }

    int64_t getLastClosedChkBySeqno() {
        LockHolder lh(queueLock);
        return lastClosedChkBySeqno;
    }

    int64_t nextBySeqno() {
        return ++lastBySeqno;
    }

private:

    bool removeCursor_UNLOCKED(const std::string &name);

    bool registerCursor_UNLOCKED(const std::string &name,
                                    uint64_t checkpointId = 1,
                                    bool alwaysFromBeginning = false);

    size_t getNumItemsForCursor_UNLOCKED(const std::string &name);

    /**
     * Create a new open checkpoint and add it to the checkpoint list.
     * The lock should be acquired before calling this function.
     * @param id the id of a checkpoint to be created.
     */
    bool addNewCheckpoint_UNLOCKED(uint64_t id);

    void removeInvalidCursorsOnCheckpoint(Checkpoint *pCheckpoint);

    /**
     * Create a new open checkpoint and add it to the checkpoint list.
     * @param id the id of a checkpoint to be created.
     */
    bool addNewCheckpoint(uint64_t id);

    bool moveCursorToNextCheckpoint(CheckpointCursor &cursor);

    /**
     * Check the current open checkpoint to see if we need to create the new open checkpoint.
     * @param forceCreation is to indicate if a new checkpoint is created due to online update or
     * high memory usage.
     * @param timeBound is to indicate if time bound should be considered in creating a new
     * checkpoint.
     * @return the previous open checkpoint Id if we create the new open checkpoint. Otherwise
     * return 0.
     */
    uint64_t checkOpenCheckpoint_UNLOCKED(bool forceCreation, bool timeBound);

    uint64_t checkOpenCheckpoint(bool forceCreation, bool timeBound) {
        LockHolder lh(queueLock);
        return checkOpenCheckpoint_UNLOCKED(forceCreation, timeBound);
    }

    bool closeOpenCheckpoint_UNLOCKED(uint64_t id);
    bool closeOpenCheckpoint(uint64_t id);

    bool isLastMutationItemInCheckpoint(CheckpointCursor &cursor);

    bool isCheckpointCreationForHighMemUsage(const RCPtr<VBucket> &vbucket);

    void collapseClosedCheckpoints(std::list<Checkpoint*> &collapsedChks);

    void collapseCheckpoints(uint64_t id);

    void resetCursors(bool resetPersistenceCursor = true);

    void putCursorsInCollapsedChk(std::map<std::string, std::pair<uint64_t, bool> > &cursors,
                                  std::list<Checkpoint*>::iterator chkItr);

    queued_item createCheckpointItem(uint64_t id, uint16_t vbid,
                                     enum queue_operation checkpoint_op);

    size_t getNumOfMetaItemsFromCursor(CheckpointCursor &cursor);

    EPStats                 &stats;
    CheckpointConfig        &checkpointConfig;
    Mutex                    queueLock;
    uint16_t                 vbucketId;
    AtomicValue<size_t>      numItems;
    int64_t                  lastBySeqno;
    int64_t                  lastClosedChkBySeqno;
    std::list<Checkpoint*>   checkpointList;
    bool                     isCollapsedCheckpoint;
    uint64_t                 lastClosedCheckpointId;
    uint64_t                 pCursorPreCheckpointId;
    cursor_index             tapCursors;
};

/**
 * A class containing the config parameters for checkpoint.
 */

class CheckpointConfig {
public:
    CheckpointConfig()
        : checkpointPeriod(DEFAULT_CHECKPOINT_PERIOD),
          checkpointMaxItems(DEFAULT_CHECKPOINT_ITEMS),
          maxCheckpoints(DEFAULT_MAX_CHECKPOINTS),
          itemNumBasedNewCheckpoint(true),
          keepClosedCheckpoints(false),
          enableChkMerge(false)
    { /* empty */ }

    CheckpointConfig(EventuallyPersistentEngine &e);

    rel_time_t getCheckpointPeriod() const {
        return checkpointPeriod;
    }

    size_t getCheckpointMaxItems() const {
        return checkpointMaxItems;
    }

    size_t getMaxCheckpoints() const {
        return maxCheckpoints;
    }

    bool isItemNumBasedNewCheckpoint() const {
        return itemNumBasedNewCheckpoint;
    }

    bool canKeepClosedCheckpoints() const {
        return keepClosedCheckpoints;
    }

    bool isCheckpointMergeSupported() const {
        return enableChkMerge;
    }

protected:
    friend class CheckpointConfigChangeListener;
    friend class EventuallyPersistentEngine;

    bool validateCheckpointMaxItemsParam(size_t checkpoint_max_items);
    bool validateCheckpointPeriodParam(size_t checkpoint_period);
    bool validateMaxCheckpointsParam(size_t max_checkpoints);

    void setCheckpointPeriod(size_t value);
    void setCheckpointMaxItems(size_t value);
    void setMaxCheckpoints(size_t value);

    void allowItemNumBasedNewCheckpoint(bool value) {
        itemNumBasedNewCheckpoint = value;
    }

    void allowKeepClosedCheckpoints(bool value) {
        keepClosedCheckpoints = value;
    }

    void allowCheckpointMerge(bool value) {
        enableChkMerge = value;
    }

    static void addConfigChangeListener(EventuallyPersistentEngine &engine);

private:
    // Period of a checkpoint in terms of time in sec
    rel_time_t checkpointPeriod;
    // Number of max items allowed in each checkpoint
    size_t checkpointMaxItems;
    // Number of max checkpoints allowed
    size_t     maxCheckpoints;
    // Flag indicating if a new checkpoint is created once the number of items in the current
    // checkpoint is greater than the max number allowed.
    bool itemNumBasedNewCheckpoint;
    // Flag indicating if closed checkpoints should be kept in memory if the current memory usage
    // below the high water mark.
    bool keepClosedCheckpoints;
    // Flag indicating if merging closed checkpoints is enabled or not.
    bool enableChkMerge;
};

#endif  // SRC_CHECKPOINT_MANAGER_H_
