
#include "config.h"

#include <cassert>
#include <iostream>

#include "linked_list.h"

class TestNode : public DLLNode {
public:
    TestNode(int d) : data(d) {}

    ~TestNode() {}

    int getData() {
        return data;
    }

private:
    int data;
};

void insertion_test() {
    DLList<TestNode> list;
    assert(list.size() == 0);
    list.pushBack(new TestNode(58));
    list.pushBack(new TestNode(39));
    list.pushBack(new TestNode(10));
    list.pushBack(new TestNode(81));
    list.pushBack(new TestNode(32));
    assert(list.size() == 5);
}

void front_test() {
    DLList<TestNode> list;
    assert(list.front() == NULL);
    list.pushBack(new TestNode(58));
    assert(list.front()->getData() == 58);
    list.pushBack(new TestNode(39));
    assert(list.front()->getData() == 58);
}

void back_test() {
    DLList<TestNode> list;
    assert(list.back() == NULL);
    list.pushBack(new TestNode(58));
    assert(list.back()->getData() == 58);
    list.pushBack(new TestNode(39));
    assert(list.back()->getData() == 39);
}

void empty_test() {
    DLList<TestNode> list;
    assert(list.empty());
    list.pushBack(new TestNode(58));
    assert(!list.empty());
}

int main() {
    insertion_test();
    front_test();
    back_test();
    empty_test();
    return 0;
}
