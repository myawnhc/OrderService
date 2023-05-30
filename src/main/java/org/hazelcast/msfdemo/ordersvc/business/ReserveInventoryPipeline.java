package org.hazelcast.msfdemo.ordersvc.business;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.grpc.GrpcService;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.ServiceFactories;
import com.hazelcast.jet.pipeline.ServiceFactory;
import com.hazelcast.jet.pipeline.Sinks;
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

        SubscriptionManager<CreditCheckEvent> eventSource = new IMapSubMgr<>("OrderEvent");
        SubscriptionManager.register(service.getHazelcastInstance(), CreditCheckEvent.class,
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
        p.readFrom(eventSource.getStreamSource("CreditCheckEvent"))
                .withoutTimestamps()
                .setName("Read CreditCheckEvents from Map Journal")

                // item here is Map.Entry<PartitionedSequenceKey,CreditCheckEvent>
                // CreditCheckEvent doesn't contain item number or location, so must enrich stream
                .mapUsingIMap(service.getOrderView(),
                        /* keyFn */ entry -> entry.getValue().getKey(),
                        /* mapFn */ (entry, order) -> tuple2(entry.getValue(), order))
                .setName("Enrich CreditCheckEvent with Order materialized view")

                // Reserve requested inventory
                // Invoke the reserve service via gRPC
                .mapUsingServiceAsync(inventoryService, (service, tuple2) -> {
                    CreditCheckEvent creditCheckEvent = tuple2.f0();
                    Order orderDO = tuple2.f1();
                    String orderNumber = creditCheckEvent.getKey();
                    InventoryOuterClass.ReserveRequest request = InventoryOuterClass.ReserveRequest.newBuilder()
                            .setItemNumber(orderDO.getItemNumber())
                            .setLocation(orderDO.getLocation())
                            .setQuantity(orderDO.getQuantity())
                            // could optionally set duration of hold but hold expiration is not implemented
                            .build();

                    // Invoke the gRPC service asynchronously
                    return service.call(request)
                            // Create the CreditCheckEvent.
                            .thenApply(response -> {
                                boolean success = response.getSuccess();
                                ReserveInventoryEvent reserve = new ReserveInventoryEvent(orderNumber,
                                        orderDO.getAcctNumber(),
                                        orderDO.getItemNumber(),
                                        orderDO.getLocation(),
                                        orderDO.getQuantity());
                                if (!success) {
                                    String failureReason = response.getReason();
                                    reserve.setFailureReason(failureReason);
                                }
                                return reserve;
                            });
                }).setName("Invoke ReserveInventory on InventoryService")

                // Call HandleEvent (append event, update materialized view, publish event)
                .mapUsingServiceAsync(eventController, (controller, creditCheckEvent) -> {
                    CompletableFuture<CompletionInfo> completion = controller.handleEvent(creditCheckEvent, UUID.randomUUID());
                    return completion;
                }).setName("Invoke EventSourcingController.handleEvent")

                // Write event to map where it will trigger subsequent pipeline stage when combined
                .writeTo(Sinks.map("ReserveInventoryEvents",
                        completionInfo -> completionInfo.getEvent().getKey(),
                        completionInfo -> completionInfo.getEvent()));

        return p;
    }
}