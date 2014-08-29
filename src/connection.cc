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

#include "connection.h"
#include "ep_engine.h"
#define STATWRITER_NAMESPACE connection
#include "statwriter.h"
#undef STATWRITER_NAMESPACE

ConnHandler::ConnHandler(EventuallyPersistentEngine& e, const void* c,
                         const std::string& n) :
    engine_(e),
    stats(engine_.getEpStats()),
    supportCheckpointSync_(false),
    name(n),
    cookie(c),
    reserved(false),
    connToken(gethrtime()),
    created(ep_current_time()),
    lastWalkTime(0),
    disconnect(false),
    connected(true),
    numDisconnects(0),
    expiryTime((rel_time_t)-1),
    supportAck(false) {}

ENGINE_ERROR_CODE ConnHandler::addStream(uint32_t opaque, uint16_t,
                                         uint32_t flags) {
    LOG(EXTENSION_LOG_WARNING, "%s Disconnecting - This connection doesn't "
        "support the dcp add stream API", logHeader());
    return ENGINE_DISCONNECT;
}

ENGINE_ERROR_CODE ConnHandler::closeStream(uint32_t opaque, uint16_t vbucket) {
    LOG(EXTENSION_LOG_WARNING, "%s Disconnecting - This connection doesn't "
        "support the dcp close stream API", logHeader());
    return ENGINE_DISCONNECT;
}

ENGINE_ERROR_CODE ConnHandler::streamEnd(uint32_t opaque, uint16_t vbucket,
                                         uint32_t flags) {
    LOG(EXTENSION_LOG_WARNING, "%s Disconnecting - This connection doesn't "
        "support the dcp stream end API", logHeader());
    return ENGINE_DISCONNECT;
}

ENGINE_ERROR_CODE ConnHandler::mutation(uint32_t opaque, const void* key,
                                        uint16_t nkey, const void* value,
                                        uint32_t nvalue, uint64_t cas,
                                        uint16_t vbucket, uint32_t flags,
                                        uint8_t datatype, uint32_t locktime,
                                        uint64_t bySeqno, uint64_t revSeqno,
                                        uint32_t exptime, uint8_t nru,
                                        const void* meta, uint16_t nmeta) {
    LOG(EXTENSION_LOG_WARNING, "%s Disconnecting - This connection doesn't "
        "support the mutation API", logHeader());
    return ENGINE_DISCONNECT;
}

ENGINE_ERROR_CODE ConnHandler::deletion(uint32_t opaque, const void* key,
                                        uint16_t nkey, uint64_t cas,
                                        uint16_t vbucket, uint64_t bySeqno,
                                        uint64_t revSeqno, const void* meta,
                                        uint16_t nmeta) {
    LOG(EXTENSION_LOG_WARNING, "%s Disconnecting - This connection doesn't "
        "support the deletion API", logHeader());
    return ENGINE_DISCONNECT;
}

ENGINE_ERROR_CODE ConnHandler::expiration(uint32_t opaque, const void* key,
                                          uint16_t nkey, uint64_t cas,
                                          uint16_t vbucket, uint64_t bySeqno,
                                          uint64_t revSeqno, const void* meta,
                                          uint16_t nmeta) {
    LOG(EXTENSION_LOG_WARNING, "%s Disconnecting - This connection doesn't "
        "support the expiration API", logHeader());
    return ENGINE_DISCONNECT;
}

ENGINE_ERROR_CODE ConnHandler::snapshotMarker(uint32_t opaque,
                                              uint16_t vbucket,
                                              uint64_t start_seqno,
                                              uint64_t end_seqno,
                                              uint32_t flags)
{
    LOG(EXTENSION_LOG_WARNING, "%s Disconnecting - This connection doesn't "
        "support the dcp snapshot marker API", logHeader());
    return ENGINE_DISCONNECT;
}

ENGINE_ERROR_CODE ConnHandler::flushall(uint32_t opaque, uint16_t vbucket) {
    LOG(EXTENSION_LOG_WARNING, "%s Disconnecting - This connection doesn't "
        "support the flush API", logHeader());
    return ENGINE_DISCONNECT;
}

ENGINE_ERROR_CODE ConnHandler::setVBucketState(uint32_t opaque,
                                               uint16_t vbucket,
                                               vbucket_state_t state) {
    LOG(EXTENSION_LOG_WARNING, "%s Disconnecting - This connection doesn't "
        "support the set vbucket state API", logHeader());
    return ENGINE_DISCONNECT;
}

ENGINE_ERROR_CODE ConnHandler::streamRequest(uint32_t flags,
                                             uint32_t opaque,
                                             uint16_t vbucket,
                                             uint64_t start_seqno,
                                             uint64_t end_seqno,
                                             uint64_t vbucket_uuid,
                                             uint64_t snapStartSeqno,
                                             uint64_t snapEndSeqno,
                                             uint64_t *rollback_seqno,
                                             dcp_add_failover_log callback) {
    LOG(EXTENSION_LOG_WARNING, "%s Disconnecting - This connection doesn't "
        "support the dcp stream request API", logHeader());
    return ENGINE_DISCONNECT;
}

ENGINE_ERROR_CODE ConnHandler::getFailoverLog(uint32_t opaque, uint16_t vbucket,
                                              dcp_add_failover_log callback) {
    LOG(EXTENSION_LOG_WARNING, "%s Disconnecting - This connection doesn't "
        "support the dcp get failover log API", logHeader());
    return ENGINE_DISCONNECT;
}

ENGINE_ERROR_CODE ConnHandler::noop(uint32_t opaque) {
    LOG(EXTENSION_LOG_WARNING, "%s Disconnecting - This connection doesn't "
        "support the noop API", logHeader());
    return ENGINE_DISCONNECT;
}

ENGINE_ERROR_CODE ConnHandler::bufferAcknowledgement(uint32_t opaque,
                                                     uint16_t vbucket,
                                                     uint32_t buffer_bytes) {
    LOG(EXTENSION_LOG_WARNING, "%s Disconnecting - This connection doesn't "
        "support the buffer acknowledgement API", logHeader());
    return ENGINE_DISCONNECT;
}

ENGINE_ERROR_CODE ConnHandler::control(uint32_t opaque, const void* key,
                                       uint16_t nkey, const void* value,
                                       uint32_t nvalue) {
    LOG(EXTENSION_LOG_WARNING, "%s Disconnecting - This connection doesn't "
        "support the control API", logHeader());
    return ENGINE_DISCONNECT;
}

ENGINE_ERROR_CODE ConnHandler::step(struct dcp_message_producers* producers) {
    LOG(EXTENSION_LOG_WARNING, "%s Disconnecting - This connection doesn't "
        "support the dcp step API", logHeader());
    return ENGINE_DISCONNECT;
}

ENGINE_ERROR_CODE ConnHandler::handleResponse(
                                        protocol_binary_response_header *resp) {
    LOG(EXTENSION_LOG_WARNING, "%s Disconnecting - This connection doesn't "
        "support the dcp response handler API", logHeader());
    return ENGINE_DISCONNECT;
}

void ConnHandler::addStats(ADD_STAT add_stat, const void *c) {
    addStat("type", getType(), add_stat, c);
    addStat("created", created, add_stat, c);
    addStat("connected", connected, add_stat, c);
    addStat("pending_disconnect", disconnect, add_stat, c);
    addStat("supports_ack", supportAck, add_stat, c);
    addStat("reserved", reserved.load(), add_stat, c);

    if (numDisconnects > 0) {
        addStat("disconnects", numDisconnects.load(), add_stat, c);
    }
}

void ConnHandler::addStat(const char *nm, const char *val, ADD_STAT add_stat,
                          const void *c) {
    add_casted_stat(nm, val, add_stat, c);
}

void ConnHandler:: addStat(const char *nm, bool val, ADD_STAT add_stat,
                           const void *c) {
    addStat(nm, val ? "true" : "false", add_stat, c);
}

void ConnHandler::releaseReference(bool force)
{
    bool inverse = true;
    if (force || reserved.compare_exchange_strong(inverse, false)) {
        engine_.releaseCookie(cookie);
    }
}
