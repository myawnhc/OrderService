package org.hazelcast.msfdemo.ordersvc.dashboard;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.crdt.pncounter.PNCounter;
import com.hazelcast.map.IMap;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public class CounterService {
    private HazelcastInstance hazelcast;
    private final PNCounter PNC_IN;
    private final PNCounter PNC_OK;
    private final PNCounter PNC_FAIL;
    private final PNCounter PNC_COMPLETE;
    private final IMap<UUID, Long> ELAPSED_TIMES;

    // TODO: Elapsed needs to be a Map keyed by UUID
    private final PNCounter PNC_TOTAL_ELAPSED;
//    private final PNCounter PNC_ELAPSED_FAIL;

    public CounterService(HazelcastInstance hz, String apiName) {
        this.hazelcast = hz;
        PNC_IN = hz.getPNCounter(apiName + "_IN");
        PNC_OK = hz.getPNCounter(apiName + "_OK");
        PNC_FAIL = hz.getPNCounter(apiName + "_FAIL");
        PNC_COMPLETE = hz.getPNCounter(apiName + "_COMPLETE");
        ELAPSED_TIMES = hz.getMap(apiName + "_ELAPSED");
        PNC_TOTAL_ELAPSED = hz.getPNCounter(apiName + "_ELAPSED");
    }

    public PNCounter getInCounter() {
        return PNC_IN;
    }

    public PNCounter getOKCounter() { return PNC_OK; }
    public PNCounter getFailCounter() { return PNC_FAIL; }
    public PNCounter getCompletedCounter() { return PNC_COMPLETE; }
    public PNCounter getTotalElapsed() { return PNC_TOTAL_ELAPSED; }
//    public PNCounter getElapsedFail() { return PNC_ELAPSED_FAIL; }

    // TODO: Record start: pass UUID and save current time
    // TODO: Record normal stop: pass UUID, calculate elapsed, add to counter

    public void startTimer(UUID uuid) {
        ELAPSED_TIMES.put(uuid, System.currentTimeMillis());
    }

    public void stopTime(UUID uuid) {
        long start = ELAPSED_TIMES.remove(uuid);
        if (start != 0) {
            long elapsed = System.currentTimeMillis() - start;
            PNC_TOTAL_ELAPSED.getAndAdd(elapsed);
        } else {
            System.out.println("Timer stopped but no start time recorded for ID " + uuid);
        }
    }

    // TODO: Maybe a separate API for failed calls - either keep a separate statistic
    //  for failed calls, or else just delete the start time and don't add to the
    //  calculated value.
}
