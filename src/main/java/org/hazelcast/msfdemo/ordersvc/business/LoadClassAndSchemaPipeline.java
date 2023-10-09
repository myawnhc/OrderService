package org.hazelcast.msfdemo.ordersvc.business;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.flakeidgen.FlakeIdGenerator;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.core.metrics.Metrics;
import com.hazelcast.jet.datamodel.Tuple2;
import com.hazelcast.jet.pipeline.BatchStage;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.ServiceFactories;
import com.hazelcast.jet.pipeline.ServiceFactory;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.StreamStage;
import com.hazelcast.jet.pipeline.test.TestSources;
import com.hazelcast.map.IMap;
import com.hazelcast.nio.serialization.genericrecord.GenericRecord;
import org.example.grpc.GrpcConnector;
import org.example.grpc.MessageWithUUID;
import org.hazelcast.eventsourcing.EventSourcingController;
import org.hazelcast.eventsourcing.sync.CompletionInfo;
import org.hazelcast.msfdemo.ordersvc.dashboard.CounterService;
import org.hazelcast.msfdemo.ordersvc.domain.Order;
import org.hazelcast.msfdemo.ordersvc.events.CollectPaymentEvent;
import org.hazelcast.msfdemo.ordersvc.events.CreateOrderEvent;
import org.hazelcast.msfdemo.ordersvc.events.CreditCheckEvent;
import org.hazelcast.msfdemo.ordersvc.events.OrderEvent;
import org.hazelcast.msfdemo.ordersvc.events.OrderOuterClass;
import org.hazelcast.msfdemo.ordersvc.events.PriceLookupEvent;
import org.hazelcast.msfdemo.ordersvc.events.PullInventoryEvent;
import org.hazelcast.msfdemo.ordersvc.events.ReserveInventoryEvent;
import org.hazelcast.msfdemo.ordersvc.events.ShipOrderEvent;
import org.hazelcast.msfdemo.ordersvc.service.OrderService;

import java.io.File;
import java.math.BigDecimal;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.logging.Logger;

import static com.hazelcast.jet.datamodel.Tuple2.tuple2;

public class LoadClassAndSchemaPipeline {
    //private static OrderService service;
    //private List<URL> dependencies;
    private static final Logger logger = Logger.getLogger(LoadClassAndSchemaPipeline.class.getName());

//    public LoadClassAndSchemaPipeline(OrderService service) {
//        LoadClassAndSchemaPipeline.service = service;
//        if (service == null)
//            throw new IllegalArgumentException("Service cannot be null");
//    }
//    @Override
//    public void run() {
//        try {
//            logger.info("LoadClassAndSchemaPipeline.run() invoked, submitting job");
//            HazelcastInstance hazelcast = service.getHazelcastInstance();
//            JobConfig jobConfig = new JobConfig();
//            jobConfig.setName("OrderService.LoadClassAndSchemaPipeline");
////            for (URL url : dependencies)
////                jobConfig.addJar(url);
//            hazelcast.getJet().newJob(createPipeline(), jobConfig);
//        } catch (Exception e) { // Happens if our pipeline is not valid
//            e.printStackTrace();
//        }
//    }

    public Pipeline createPipeline() {
        Pipeline p = Pipeline.create();

        ServiceFactory<?, IMap<String, GenericRecord>> mapService =
                ServiceFactories.iMapService("dummy");

        // Keys are arbitrary and irrelevant
        BatchStage<String> keys = p.readFrom(TestSources.items("1", "2", "3", "4", "5", "6", "7"));

        // This stage forces serialization of the various object types
        BatchStage<Tuple2<String,OrderEvent>> events = keys.map(key -> {
            switch(key) {
                case "1":
                    return Tuple2.tuple2(key, new CreateOrderEvent("1", "2", "3", "4", 5));
                case "2":
                    return Tuple2.tuple2(key, new PriceLookupEvent("1", "2", "3", 4, new BigDecimal(5.67)));
                case "3":
                    return Tuple2.tuple2(key, new CreditCheckEvent("1", "2", new BigDecimal(12.34), true));
                case "4":
                    return Tuple2.tuple2(key, new ReserveInventoryEvent("1", "2", "3", "4", 5));
                case "5":
                    return Tuple2.tuple2(key, new CollectPaymentEvent("1", "2", new BigDecimal(11.22)));
                case "6":
                    return Tuple2.tuple2(key, new PullInventoryEvent("1", "2", "3", "4", 5));
                case "7":
                    return Tuple2.tuple2(key, new ShipOrderEvent("1", "2", "3", 4));
            }
            return null;
        });

        // This stage will convert event classes to GenericRecords using Compact Serialization;
        // it's possible the classloading deadlock will be triggered here although 'get' seems
        // to be more common
        BatchStage<String> keys2 = events.mapUsingService(mapService, (map, tuple) -> {
            map.put(tuple.f0(), tuple.f1().toGenericRecord());
            return tuple.f0();
        });

        BatchStage<String> strings = keys2.mapUsingService(mapService, (map, key) -> {
            GenericRecord gr = map.get(key);
            switch(key) {
                case "1":
                    CreateOrderEvent coe = new CreateOrderEvent(gr);
                    return coe.toString();
                case "2":
                    PriceLookupEvent ple = new PriceLookupEvent(gr);
                    return ple.toString();
                case "3":
                    CreditCheckEvent cce = new CreditCheckEvent(gr);
                    return cce.toString();
                case "4":
                    ReserveInventoryEvent rie = new ReserveInventoryEvent(gr);
                    return rie.toString();
                case "5":
                    CollectPaymentEvent cpe = new CollectPaymentEvent(gr);
                    return cpe.toString();
                case "6":
                    PullInventoryEvent pie = new PullInventoryEvent(gr);
                    return pie.toString();
                case "7":
                    ShipOrderEvent soe = new ShipOrderEvent(gr);
                    return soe.toString();
            }
            return "unimplemented";
        });

        strings.writeTo(Sinks.logger());

        return p;
    }

    public static void main(String[] args) throws MalformedURLException {
        HazelcastInstance hazelcast = Hazelcast.newHazelcastInstance();
        LoadClassAndSchemaPipeline main = new LoadClassAndSchemaPipeline();
        Pipeline p = main.createPipeline();
        File esJar = new File("target/dependentJars/eventsourcing-1.0-SNAPSHOT.jar");
        URL es = esJar.toURI().toURL();
        JobConfig jobConfig = new JobConfig();
        jobConfig.setName("Reproducer for classloader deadlock");
        jobConfig.addJar(es);
        hazelcast.getJet().newJob(p, jobConfig);
    }
}
