package org.hazelcast.msfdemo.ordersvc.business;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.core.metrics.Metrics;
import com.hazelcast.jet.grpc.GrpcService;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.ServiceFactories;
import com.hazelcast.jet.pipeline.ServiceFactory;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.nio.serialization.genericrecord.GenericRecord;
import io.grpc.ManagedChannelBuilder;
import org.hazelcast.eventsourcing.EventSourcingController;
import org.hazelcast.eventsourcing.pubsub.SubscriptionManager;
import org.hazelcast.eventsourcing.pubsub.impl.IMapSubMgr;
import org.hazelcast.eventsourcing.sync.CompletionInfo;
import org.hazelcast.msfdemo.acctsvc.events.AccountGrpc;
import org.hazelcast.msfdemo.acctsvc.events.AccountOuterClass;
import org.hazelcast.msfdemo.invsvc.events.InventoryGrpc;
import org.hazelcast.msfdemo.invsvc.events.InventoryOuterClass;
import org.hazelcast.msfdemo.ordersvc.configuration.ServiceConfig;
import org.hazelcast.msfdemo.ordersvc.domain.Order;
import org.hazelcast.msfdemo.ordersvc.events.CreditCheckEvent;
import org.hazelcast.msfdemo.ordersvc.events.OrderEvent;
import org.hazelcast.msfdemo.ordersvc.events.PriceLookupEvent;
import org.hazelcast.msfdemo.ordersvc.events.ReserveInventoryEvent;
import org.hazelcast.msfdemo.ordersvc.service.OrderService;

import java.net.URL;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.logging.Logger;

import static com.hazelcast.jet.datamodel.Tuple2.tuple2;
import static com.hazelcast.jet.grpc.GrpcServices.unaryService;

public class ReserveInventoryPipeline implements Runnable {

    private static OrderService service;
    private List<URL> dependencies;
    private static String inventoryServiceHost;
    private static int inventoryServicePort;
    private static final Logger logger = Logger.getLogger(ReserveInventoryPipeline.class.getName());

    public ReserveInventoryPipeline(OrderService service, byte[] clientConfig, List<URL> dependentJars) {
        ReserveInventoryPipeline.service = service;
        if (service == null)
            throw new IllegalArgumentException("Service cannot be null");
        // When running in client/server mode, service won't be initialized yet
        if (service.getEventSourcingController() == null && clientConfig != null) {
            service.initService(clientConfig);
        }
        this.dependencies = dependentJars;

        // Foreign service configuration
        ServiceConfig.ServiceProperties props = ServiceConfig.get("inventory-service");
        inventoryServiceHost = props.getGrpcHostname();
        inventoryServicePort = props.getGrpcPort();
    }

    @Override
    public void run() {
        try {
            logger.info("ReserveInventoryPipeline.run() invoked, submitting job");
            HazelcastInstance hazelcast = service.getHazelcastInstance();
            JobConfig jobConfig = new JobConfig();
            jobConfig.setName("OrderService.ReserveInventory");
            for (URL url : dependencies)
                jobConfig.addJar(url);
            hazelcast.getJet().newJob(createPipeline(), jobConfig);
        } catch (Exception e) { // Happens if our pipeline is not valid
            e.printStackTrace();
        }
    }

    private static Pipeline createPipeline() {
        Pipeline p = Pipeline.create();

        SubscriptionManager<PriceLookupEvent> eventSource = new IMapSubMgr<>("OrderEvent");
        SubscriptionManager.register(service.getHazelcastInstance(), PriceLookupEvent.QUAL_EVENT_NAME,
                eventSource);

        ServiceFactory<?, ? extends GrpcService<InventoryOuterClass.ReserveRequest, InventoryOuterClass.ReserveResponse>>
                inventoryService = unaryService(
                () -> ManagedChannelBuilder.forAddress(inventoryServiceHost, inventoryServicePort).usePlaintext(),
                channel -> InventoryGrpc.newStub(channel)::reserve
        );

        // EventSourcingController will add event to event store, update the in-memory
        // materialized view, and publish the event to all subscribers
        ServiceFactory<?, EventSourcingController<Order,String,OrderEvent>> eventController =
                ServiceFactories.sharedService(
                        (ctx) -> service.getEventSourcingController()
                );

        // Read events from MapJournal
        p.readFrom(eventSource.getStreamSource(PriceLookupEvent.QUAL_EVENT_NAME))
                .withoutTimestamps()
                .setName("Read PriceLookupEvents from Map Journal")

                // item here is Map.Entry<PartitionedSequenceKey,CreditCheckEvent>
                // CreditCheckEvent doesn't contain item number or location, so must enrich stream
                .mapUsingIMap(service.getOrderView(),
                        /* keyFn */ entry -> entry.getValue().getKey(),
                        /* mapFn */ (entry, orderGR) -> {
                            //Order order = new Order(orderGR);
                            //OK logger.info("ReserveInventoryPipeline builds " + order + " from " + orderGR);
                            return tuple2(entry.getValue(), new Order(orderGR));
                        })
                 .setName("Enrich CreditCheckEvent with Order materialized view")

                // Reserve requested inventory
                // Invoke the reserve service via gRPC
                .mapUsingServiceAsync(inventoryService, (invsvc, tuple2) -> {
                    //OK logger.info("ReserveInventoryPipeline building request");
                    PriceLookupEvent priceLookupEvent = tuple2.f0();
                    Order orderDO = tuple2.f1();
                    String orderNumber = priceLookupEvent.getKey();
                    InventoryOuterClass.ReserveRequest request = InventoryOuterClass.ReserveRequest.newBuilder()
                            .setItemNumber(orderDO.getItemNumber())
                            .setLocation(orderDO.getLocation())
                            .setQuantity(orderDO.getQuantity())
                            // could optionally set duration of hold but hold expiration is not implemented
                            .build();
                    logger.info("ReserveInventoryPipeline calling ReserveInventory remote service");

                    // Invoke the gRPC service asynchronously
                    return invsvc.call(request)
                            // Create the ReserviceInventoryEvent once we have gRPC response
                            .thenApply(response -> {
                                logger.info("Received response from InventoryService::ReserveInventory");
                                boolean success = response.getSuccess();
                                ReserveInventoryEvent reserve = new ReserveInventoryEvent(orderNumber,
                                        orderDO.getAcctNumber(),
                                        orderDO.getItemNumber(),
                                        orderDO.getLocation(),
                                        orderDO.getQuantity());
                                if (!success) {
                                    String failureReason = response.getReason();
                                    reserve.setFailureReason(failureReason);
                                    logger.warning("   ReserveInventory denied " + failureReason);
                                }
                                //Metrics.metric("RIP.serviceCallCompleted").increment();
                                return reserve;
                            });
                }).setName("Invoke ReserveInventory on InventoryService")

                // Call HandleEvent (append event, update materialized view, publish event)
                .mapUsingServiceAsync(eventController, (controller, reserveInventoryEvent) -> {
                    CompletableFuture<CompletionInfo> completion = controller.handleEvent(reserveInventoryEvent, UUID.randomUUID());
                    logger.info("ReserveInventoryPipeline received Future from ESController.handleEvent");
                    Metrics.metric("RIP.handleEventCalled").increment();
                    return completion;
                }).setName("Invoke EventSourcingController.handleEvent")

                // Write event to map where it will trigger subsequent pipeline stage when combined
                .writeTo(Sinks.map("ReserveInventoryEvents",
                        completionInfo -> completionInfo.getEvent().getKey(),
                        //completionInfo -> completionInfo.getEvent()));
                        completionInfo -> {
                            logger.info("RIP.handleEvent finished, writing to ReserveInventoryEvents");
                            Metrics.metric("RIP.completions").increment();
                            return completionInfo.getEvent();
                        }));
        return p;
    }
}