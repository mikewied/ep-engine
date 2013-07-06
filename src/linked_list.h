
#ifndef SRC_LINKED_LIST_H_
#define SRC_LINKED_LIST_H_ 1

#include "config.h"

class DLLNode {
public:
    DLLNode() : prev(NULL), next(NULL) {}
    ~DLLNode() {}

    DLLNode* prev;
    DLLNode* next;
};

template <typename T>
class DLList {
public:
    DLList() : head(NULL), tail(NULL), length(0) {}

    ~DLList() {}

    T* front() {
        return reinterpret_cast<T*>(head);
    }

    T* back() {
        return reinterpret_cast<T*>(tail);
    }

    void pushBack(T* data) {
        //DLLNode* node = reinterpret_cast<DLLNode*>(data);
        if (empty()) {
            head = data;
            tail = data;
        } else {
            data->prev = tail;
            data->next = NULL;
            tail = data;
        }
        ++length;
    }

    bool empty() {
        return (!head && !tail);
    }

    size_t size() {
        return length;
    }

private:
    T* head;
    T* tail;
    size_t length;
};

#endif  // SRC_LINKED_LIST_H_
