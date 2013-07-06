
#ifndef SRC_RWLOCK_H_
#define SRC_RWLOCK_H_ 1
#include "config.h"

class RWLock {
public:
    RWLock() {
        int rc;
#ifdef VALGRIND
        memset(&rwlock, 0, sizeof(rwlock));
#endif
        if ((rc = pthread_rwlock_init(&rwlock, NULL))) {
            std::cerr << "RWLOCK ERROR: Failed to initialize rwlock: ";
            std::cerr << std::strerror(rc) << std::endl;
            abort();
        }
    }

    void aquireReader() {
        int rc;
        if ((rc = pthread_rwlock_rdlock(&rwlock))) {
            std::cerr << "RWLOCK ERROR: Error getting reader lock: ";
            std::cerr << std::strerror(rc) << std::endl;
            abort();
        }
    }

    void aquireWriter() {
        int rc;
        if ((rc = pthread_rwlock_wrlock(&rwlock))) {
            std::cerr << "RWLOCK ERROR: Error getting writer lock: ";
            std::cerr << std::strerror(rc) << std::endl;
            abort();
        }
    }

    bool tryAquireReader() {
        int rc = pthread_rwlock_tryrdlock(&rwlock);
        if (rc = EBUSY) {
            return false;
        } else {
            std::cerr << "RWLOCK ERROR: Error trying to aquire reader lock: ";
            std::cerr << std::strerror(rc) << std::endl;
            abort();
        }
        return true;
    }

    bool tryAquireWriter() {
        int rc = pthread_rwlock_trywrlock(&rwlock);
        if (rc = EBUSY) {
            return false;
        } else {
            std::cerr << "RWLOCK ERROR: Error trying to aquire writer lock: ";
            std::cerr << std::strerror(rc) << std::endl;
            abort();
        }
        return true;
    }

    void unlock() {
        int rc;
        if ((rc = pthread_rwlock_unlock(&rwlock))) {
            std::cerr << "RWLOCK ERROR: Error unlocking rwlock: ";
            std::cerr << std::strerror(rc) << std::endl;
            abort();
        }
    }

    ~RWLock() {
        int rc;
        if ((rc = pthread_rwlock_destroy(&rwlock))) {
            std::cerr << "RWLOCK ERROR: Failed to destroy rwlock: ";
            std::cerr << std::strerror(rc) << std::endl;
            abort();
        }
    }

private:
    pthread_rwlock_t  rwlock;

    DISALLOW_COPY_AND_ASSIGN(RWLock);
};

#endif  // SRC_RWLOCK_H_
