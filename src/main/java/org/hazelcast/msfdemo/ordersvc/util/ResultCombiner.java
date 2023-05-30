package org.hazelcast.msfdemo.ordersvc.util;

import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.datamodel.Tuple2;
import com.hazelcast.map.IMap;
import com.hazelcast.map.listener.EntryAddedListener;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class ResultCombiner<K> {
    // Goals:
    // - Allow combining results from 2 or more pipelines
    //   - Initial implementation will expect results to be in IMaps and use listeners to detect new results
    //   - In theory could also accept results from topic, queue, Kafka, etc.
    //   - Initial implementation assumes all results required ('AND'), but could accept additional conditions
    //     (OR condition, all that complete within a time limit, etc.)
    //   - Default output is a tuple of input results, but can accept a transform function to produce
    //     whatever output format is required

    private HazelcastInstance hazelcast;

    // Partial results list.
    private int inputCount = 0;
    Map<K, Object[]> partialResults;
    List<String> inputMapNames = new ArrayList<>();
    String outputMapName;
    Map<K, Tuple2> outputMap;

    // partial results -- key column, then 1 column per expected result
    // perhaps map <keyType, array[object] results (objects may be different types]
    // somewhere associate index - value class - map name

    public ResultCombiner(HazelcastInstance hazelcast) {
        this.hazelcast = hazelcast;
        partialResults = hazelcast.getMap("partialResults");
    }


    // may eventually be renamed 'addMapInput' if we support other input types
    public <K,V> ResultCombiner addInput(String mapName) {
        IMap<K,V> map = hazelcast.getMap(mapName);
        map.addEntryListener(new Listener<V>(inputCount), true);
        inputMapNames.add(mapName);
        //System.out.println("Results from " + mapName + " will be stored in column " + inputCount);
        inputCount++;
        return this;
    }

    public ResultCombiner addOutput(String mapName) {
        outputMapName = mapName;
        outputMap = hazelcast.getMap(outputMapName);
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
            //System.out.println("Added " + key + "," + value + " at index " + index);
            Object[] resultsSoFar = partialResults.get(key);
            if (resultsSoFar == null)
                resultsSoFar = new Object[inputCount];
            resultsSoFar[index] = value;
            partialResults.put(key, resultsSoFar);
            // This should be a function that can be replaced for different completion
            //  conditions (all of, one of, etc.)
            //boolean isComplete = true;
            for (int i=0; i<inputCount; i++) {
                if (resultsSoFar[i] == null) {
                    //isComplete = false;
                    //System.out.println("Missing result " + i + " for key " + key);
                    return;
                }
            }
            // if all required inputs have been received, produce output
            // else add this input to the list of pending inputs
            //System.out.println("Results complete for key " + key);
            Tuple2 results = produceResult(resultsSoFar);
            //System.out.println("Produced result: " + results);
            outputMap.put(key, results);
            for (int i=0; i<inputCount; i++) {
                Map inputMap = hazelcast.getMap(inputMapNames.get(i));
                inputMap.remove(key);
            }
            partialResults.remove(key);
        }
    }

    // We want to put result to a map so that the map journal can be used as input to another pipeline...
    // but might still want to separate the 'combine the result' and 'publish the result' steps so they
    // can be independently evolved to support different workflows.
    @Deprecated // in favor of version that uses partialResults map
    private <T,U> Tuple2<T,U> produceResult(T input1, U input2) {
        return Tuple2.tuple2(input1, input2);
    }

    // TODO: will eventually make this specific to the number of inputs
    private Tuple2 produceResult(Object[] inputs) {
        return Tuple2.tuple2(inputs[0], inputs[1]);
    }

    // Quick test
    public static void main(String[] args) {
        HazelcastInstance hz = Hazelcast.newHazelcastInstance();
        ResultCombiner<String> combiner = new ResultCombiner<>(hz);
        combiner.<String,Float>addInput("floatMap")
                .<String,Integer>addInput("intMap")
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
