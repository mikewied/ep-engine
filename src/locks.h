/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2010 Couchbase, Inc
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

#ifndef SRC_LOCKS_H_
#define SRC_LOCKS_H_ 1

#include "config.h"

#include <functional>
#include <iostream>
#include <sstream>
#include <stdexcept>

#include "common.h"
#include "mutex.h"
#include "syncobject.h"

/**
 * RAII lock holder to guarantee release of the lock.
 *
 * It is a very bad idea to unlock a lock held by a LockHolder without
 * using the LockHolder::unlock method.
 */
class LockHolder {
public:
    /**
     * Acquire the lock in the given mutex.
     */
    LockHolder(Mutex &m) : mutex(m), locked(false) {
        lock();
    }

    /**
     * Copy constructor hands this lock to the new copy and then
     * consider it released locally (i.e. renders unlock() a noop).
     */
    LockHolder(const LockHolder& from) : mutex(from.mutex), locked(true) {
        const_cast<LockHolder*>(&from)->locked = false;
    }

    /**
     * Release the lock.
     */
    ~LockHolder() {
        unlock();
    }

    /**
     * Relock a lock that was manually unlocked.
     */
    void lock() {
        mutex.acquire();
        locked = true;
    }

    /**
     * Manually unlock a lock.
     */
    void unlock() {
        if (locked) {
            locked = false;
            mutex.release();
        }
    }

private:
    Mutex &mutex;
    bool locked;

    void operator=(const LockHolder&);
};

/**
 * RAII lock holder over multiple locks.
 */
class MultiLockHolder {
public:

    /**
     * Acquire a series of locks.
     *
     * @param m beginning of an array of locks
     * @param n the number of locks to lock
     */
    MultiLockHolder(Mutex *m, size_t n) : mutexes(m),
                                          locked(new bool[n]),
                                          n_locks(n) {
        std::fill_n(locked, n_locks, false);
        lock();
    }

    ~MultiLockHolder() {
        unlock();
        delete[] locked;
    }

    /**
     * Relock the series after having manually unlocked it.
     */
    void lock() {
        for (size_t i = 0; i < n_locks; i++) {
            cb_assert(!locked[i]);
            mutexes[i].acquire();
            locked[i] = true;
        }
    }

    /**
     * Manually unlock the series.
     */
    void unlock() {
        for (size_t i = 0; i < n_locks; i++) {
            if (locked[i]) {
                locked[i] = false;
                mutexes[i].release();
            }
        }
    }

private:
    Mutex  *mutexes;
    bool   *locked;
    size_t  n_locks;

    DISALLOW_COPY_AND_ASSIGN(MultiLockHolder);
};

#endif  // SRC_LOCKS_H_
