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

#ifndef SRC_CONNECTION_H_
#define SRC_CONNECTION_H_ 1

#include "config.h"

#include "atomic.h"

class EventuallyPersistentEngine;
class EPStats;

typedef enum {
    TAP_CONN, //!< TAP connnection
    DCP_CONN  //!< DCP connection
} conn_type_t;

/**
 * Aggregator object to count all tap stats.
 */
struct ConnCounter {
    ConnCounter()
        : conn_queue(0), totalConns(0), totalProducers(0),
          conn_queueFill(0), conn_queueDrain(0), conn_totalBytes(0), conn_queueRemaining(0),
          conn_queueBackoff(0), conn_queueBackfillRemaining(0), conn_queueItemOnDisk(0),
          conn_totalBacklogSize(0)
    {}

    ConnCounter& operator+=(const ConnCounter& other) {
        conn_queue += other.conn_queue;
        totalConns += other.totalConns;
        totalProducers += other.totalProducers;
        conn_queueFill += other.conn_queueFill;
        conn_queueDrain += other.conn_queueDrain;
        conn_totalBytes += other.conn_totalBytes;
        conn_queueRemaining += other.conn_queueRemaining;
        conn_queueBackoff += other.conn_queueBackoff;
        conn_queueBackfillRemaining += other.conn_queueBackfillRemaining;
        conn_queueItemOnDisk += other.conn_queueItemOnDisk;
        conn_totalBacklogSize += other.conn_totalBacklogSize;

        return *this;
    }

    size_t      conn_queue;
    size_t      totalConns;
    size_t      totalProducers;

    size_t      conn_queueFill;
    size_t      conn_queueDrain;
    size_t      conn_totalBytes;
    size_t      conn_queueRemaining;
    size_t      conn_queueBackoff;
    size_t      conn_queueBackfillRemaining;
    size_t      conn_queueItemOnDisk;
    size_t      conn_totalBacklogSize;
};

class ConnHandler : public RCValue {
public:
    ConnHandler(EventuallyPersistentEngine& engine, const void* c,
                const std::string& name);

    virtual ~ConnHandler() {}

    virtual ENGINE_ERROR_CODE addStream(uint32_t opaque, uint16_t vbucket,
                                        uint32_t flags);

    virtual ENGINE_ERROR_CODE closeStream(uint32_t opaque, uint16_t vbucket);

    virtual ENGINE_ERROR_CODE streamEnd(uint32_t opaque, uint16_t vbucket,
                                        uint32_t flags);

    virtual ENGINE_ERROR_CODE mutation(uint32_t opaque, const void* key,
                                       uint16_t nkey, const void* value,
                                       uint32_t nvalue, uint64_t cas,
                                       uint16_t vbucket, uint32_t flags,
                                       uint8_t datatype, uint32_t locktime,
                                       uint64_t bySeqno, uint64_t revSeqno,
                                       uint32_t exptime, uint8_t nru,
                                       const void* meta, uint16_t nmeta);

    virtual ENGINE_ERROR_CODE deletion(uint32_t opaque, const void* key,
                                       uint16_t nkey, uint64_t cas,
                                       uint16_t vbucket, uint64_t bySeqno,
                                       uint64_t revSeqno, const void* meta,
                                       uint16_t nmeta);

    virtual ENGINE_ERROR_CODE expiration(uint32_t opaque, const void* key,
                                         uint16_t nkey, uint64_t cas,
                                         uint16_t vbucket, uint64_t bySeqno,
                                         uint64_t revSeqno, const void* meta,
                                         uint16_t nmeta);

    virtual ENGINE_ERROR_CODE snapshotMarker(uint32_t opaque,
                                             uint16_t vbucket,
                                             uint64_t start_seqno,
                                             uint64_t end_seqno,
                                             uint32_t flags);

    virtual ENGINE_ERROR_CODE flushall(uint32_t opaque, uint16_t vbucket);

    virtual ENGINE_ERROR_CODE setVBucketState(uint32_t opaque, uint16_t vbucket,
                                              vbucket_state_t state);

    virtual ENGINE_ERROR_CODE getFailoverLog(uint32_t opaque, uint16_t vbucket,
                                             dcp_add_failover_log callback);

    virtual ENGINE_ERROR_CODE streamRequest(uint32_t flags,
                                            uint32_t opaque,
                                            uint16_t vbucket,
                                            uint64_t start_seqno,
                                            uint64_t end_seqno,
                                            uint64_t vbucket_uuid,
                                            uint64_t snapStartSeqno,
                                            uint64_t snapEndSeqno,
                                            uint64_t *rollback_seqno,
                                            dcp_add_failover_log callback);

    virtual ENGINE_ERROR_CODE noop(uint32_t opaque);

    virtual ENGINE_ERROR_CODE bufferAcknowledgement(uint32_t opaque,
                                                    uint16_t vbucket,
                                                    uint32_t buffer_bytes);

    virtual ENGINE_ERROR_CODE control(uint32_t opaque, const void* key,
                                      uint16_t nkey, const void* value,
                                      uint32_t nvalue);

    virtual ENGINE_ERROR_CODE step(struct dcp_message_producers* producers);

    virtual ENGINE_ERROR_CODE handleResponse(
                                        protocol_binary_response_header *resp);

    const char* logHeader() {
        return logString.c_str();
    }

    void setLogHeader(const std::string &header) {
        logString = header;
    }

    void releaseReference(bool force = false);

    virtual const char *getType() const = 0;

    template <typename T>
    void addStat(const char *nm, const T &val, ADD_STAT add_stat, const void *c) {
        std::stringstream tap;
        tap << name << ":" << nm;
        std::stringstream value;
        value << val;
        std::string n = tap.str();
        addStat(n.data(), value.str().data(), add_stat, c);
    }

    void addStat(const char *nm, const char *val, ADD_STAT add_stat, const void *c);

    void addStat(const char *nm, bool val, ADD_STAT add_stat, const void *c);

    virtual void addStats(ADD_STAT add_stat, const void *c);

    virtual void aggregateQueueStats(ConnCounter* stats_aggregator) {
        // Empty
    }

    const std::string &getName() const {
        return name;
    }

    void setName(const std::string &n) {
        name.assign(n);
    }

    bool setReserved(bool r) {
        bool inverse = !r;
        return reserved.compare_exchange_strong(inverse, r);
    }

    bool isReserved() const {
        return reserved;
    }

    const void *getCookie() const {
        return cookie;
    }

    void setCookie(const void *c) {
        cookie = c;
    }

    void setLastWalkTime() {
        lastWalkTime = ep_current_time();
    }

    rel_time_t getLastWalkTime() {
        return lastWalkTime;
    }

    void setConnected(bool s) {
        if (!s) {
            ++numDisconnects;
        }
        connected = s;
    }

    bool isConnected() {
        return connected;
    }

    bool doDisconnect() {
        return disconnect;
    }

    virtual void setDisconnect(bool val) {
        disconnect = val;
    }

protected:
    EventuallyPersistentEngine &engine_;
    EPStats &stats;

private:

     //! The name for this connection
    std::string name;

    //! The string used to prefix all log messages for this connection
    std::string logString;

    //! The cookie representing this connection (provided by the memcached code)
    const void* cookie;

    //! Whether or not the connection is reserved in the memcached layer
    AtomicValue<bool> reserved;

    //! Connection creation time
    rel_time_t created;

    //! The last time this connection's step function was called
    rel_time_t lastWalkTime;

    //! Should we disconnect as soon as possible?
    bool disconnect;

    //! Is this tap conenction connected?
    bool connected;

    //! Number of times this connection was disconnected
    AtomicValue<size_t> numDisconnects;
};

class Notifiable {
public:
    Notifiable()
      : suspended(false), paused(false),
        notificationScheduled(false), notifySent(false) {}

    virtual ~Notifiable() {}

    bool isPaused() {
        return paused;
    }

    void setPaused(bool p) {
        paused.store(p);
    }

    bool isNotificationScheduled() {
        return notificationScheduled;
    }

    bool setNotificationScheduled(bool val) {
        bool inverse = !val;
        return notificationScheduled.compare_exchange_strong(inverse, val);
    }

    bool setNotifySent(bool val) {
        bool inverse = !val;
        return notifySent.compare_exchange_strong(inverse, val);
    }

    bool sentNotify() {
        return notifySent;
    }

    virtual void setSuspended(bool value) {
        suspended = value;
    }

    bool isSuspended() {
        return suspended;
    }

private:
    //! Is this tap connection in a suspended state
    bool suspended;
    //! Connection is temporarily paused?
    AtomicValue<bool> paused;
    //! Flag indicating if the notification event is scheduled
    AtomicValue<bool> notificationScheduled;
        //! Flag indicating if the pending memcached connection is notified
    AtomicValue<bool> notifySent;
};

#endif  // SRC_CONNECTION_H_
