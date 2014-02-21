#include "cybozu/test.hpp"
#include "state_machine.hpp"

CYBOZU_TEST_AUTO(add)
{
	walb::StateMachine sm;
	sm.addEdge("a", "b");
	sm.addEdge("a", "c");
	sm.addEdge("a", "d");
	sm.addEdge("b", "c");
	CYBOZU_TEST_EXCEPTION(sm.get(), cybozu::Exception);
	sm.set("a");
	CYBOZU_TEST_EQUAL(sm.get(), "a");
	CYBOZU_TEST_EXCEPTION(sm.set("none"), cybozu::Exception);
}
