package org.hazelcast.msfdemo.ordersvc.util;

import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.datamodel.Tuple2;
import com.hazelcast.map.IMap;
import com.hazelcast.map.listener.EntryAddedListener;

import java.util.Map;
import java.util.logging.Logger;

public class ResultCombiner<K,Y,Z> {

    private HazelcastInstance hazelcast;

    IMap<K, Tuple2<Y,Z>> partialResults;
    String[] inputMapNames = new String[2];
    String outputMapName;
    IMap<K, Y> inputMap1;
    IMap<K, Z> inputMap2;
    IMap<K, Tuple2<Y,Z>> outputMap;
    private static final Logger logger = Logger.getLogger(ResultCombiner.class.getName());

    public ResultCombiner(HazelcastInstance hazelcast) {
        this.hazelcast = hazelcast;
        partialResults = hazelcast.getMap("partialResults");
    }

    public ResultCombiner addInputs(String map1name, String map2name) {
        inputMapNames[0] = map1name;
        inputMap1 = hazelcast.getMap(map1name);
        inputMapNames[1] = map2name;
        inputMap2 = hazelcast.getMap(map2name);
        inputMap1.addEntryListener(new Listener<Y>(0), true);
        inputMap2.addEntryListener(new Listener<Z>(1), true);
        logger.info("ResultCombiner armed listeners for maps " + map1name + " and " + map2name);
        return this;
    }


    public ResultCombiner addOutput(String mapName) {
        outputMapName = mapName;
        outputMap = hazelcast.getMap(outputMapName);
        partialResults = hazelcast.getMap(outputMapName + "_partial");
        return this;
    }

    private class Listener<V> implements EntryAddedListener<K,V> {
        private int index;
        public Listener(int index) {
            this.index = index;
        }

        @Override
        public void entryAdded(EntryEvent<K, V> entryEvent) {
            K key = entryEvent.getKey();
            V value = entryEvent.getValue();
            //System.out.println("ResultCombiner for " + outputMapName + " : Added " + value.getClass().getSimpleName() + " at index " + index);
            Tuple2<Y,Z> resultsSoFar = partialResults.get(key);
            if (resultsSoFar == null) {
                // This is the first result; store it and exit
                if (index == 0)
                    partialResults.put(key, Tuple2.tuple2((Y)value, null));
                else
                    partialResults.put(key, Tuple2.tuple2(null, (Z)value));
            } else {
                // This is the second/final result, write combo
                Tuple2<Y,Z> combo;
                if (index == 0)
                    combo = Tuple2.tuple2((Y)value, resultsSoFar.f1());
                else
                    combo = Tuple2.tuple2(resultsSoFar.f0(), (Z) value);
                outputMap.put(key, combo);
                // Remove inputs
                inputMap1.remove(key);
                inputMap2.remove(key);
                partialResults.remove(key);
                //logger.info("Wrote to " + outputMapName + " [0: " + combo.f0().getClass().getSimpleName() + "] " +
                //        " [1: " + combo.f1().getClass().getSimpleName() + "]");
                partialResults.remove(key);
            }
        }
    }

    // Quick test
    public static void main(String[] args) {
        HazelcastInstance hz = Hazelcast.newHazelcastInstance();
        ResultCombiner<String,Float,Integer> combiner = new ResultCombiner<>(hz);
        combiner.addInputs("floatMap", "intMap")
                .addOutput("outputMap");
        Map<String,Integer> intMap = hz.getMap("intMap");
        Map<String,Float> floatMap = hz.getMap("floatMap");
        intMap.put("1", 1);
        intMap.put("3", 3);
        floatMap.put("2", 2.0F);
        intMap.put("2", 2);
        floatMap.put("3", 3.0F);
        floatMap.put("1", 1.0F);
    }
}
