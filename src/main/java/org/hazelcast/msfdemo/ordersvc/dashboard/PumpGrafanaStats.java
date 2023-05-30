/*
 *  Copyright 2018-2021 Hazelcast, Inc
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.package com.theyawns.controller.launcher;
 */

package org.hazelcast.msfdemo.ordersvc.dashboard;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.scheduledexecutor.NamedTask;

import java.io.IOException;
import java.io.Serializable;

/** Runnable task that pumps stats from several PNCounters to Graphite / Grafana */
public class PumpGrafanaStats implements Serializable, Runnable, HazelcastInstanceAware, NamedTask {

    private transient HazelcastInstance hazelcast;
    private transient Graphite graphite;
    private boolean initialized = false;
    private String host;

    public PumpGrafanaStats(String host) {
        this.host = host;
    }

    private void init() {
        graphite = new Graphite(host);
        initialized = true;
    }

    // Runs at intervals
    @Override
    public void run() {
    	//System.out.println("PumpGrafanaStats active");
        if (!initialized)
            init();

        String prefix1 = "order.";
        String[] pipelines = new String[] { "CreateOrder" };
        String[] prefix2 = new String[] { "create." };

        for (int i=0; i<pipelines.length; i++) {
            CounterService counters = new CounterService(hazelcast, pipelines[i]);
            String prefix = prefix1 + prefix2[i];
            try {
                long inVal = counters.getInCounter().get();
                //System.out.println("sending IN value " + inVal);
                graphite.writeStats(prefix + "IN", inVal);
                long okVal = counters.getOKCounter().get();
                //System.out.println("sending OK value " + okVal);
                graphite.writeStats(prefix + "OK", okVal);
                graphite.writeStats(prefix + "FAIL", counters.getFailCounter().get());
                long compVal = counters.getCompletedCounter().get();
                graphite.writeStats(prefix + "COMPLETE", compVal);
                double avgLatencyOK = ((double) counters.getTotalElapsed().get()) / ((double) counters.getOKCounter().get());
                graphite.writeStats(prefix + "LATENCY_OK", avgLatencyOK);
                System.out.println("PumpGrafanaStats sending avg " + pipelines[i] + " latency value " + avgLatencyOK + ", completions " + compVal);


            } catch (IOException e) {
                e.printStackTrace();
                System.out.println("** Reinitializing Graphite");
                graphite = new Graphite(host);
            }
        }
        //lastTimeRun = System.currentTimeMillis();
        //System.out.println("PumpGrafanaStats complete");
    }

    @Override
    public void setHazelcastInstance(HazelcastInstance hazelcastInstance) {
        this.hazelcast = hazelcastInstance;
        init();
    }

    @Override
    public String getName() {
        return "PumpGrafanaStats";
    }
}
