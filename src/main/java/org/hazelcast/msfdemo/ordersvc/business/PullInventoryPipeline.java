package org.hazelcast.msfdemo.ordersvc.business;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.datamodel.Tuple2;
import com.hazelcast.jet.grpc.GrpcService;
import com.hazelcast.jet.pipeline.JournalInitialPosition;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.ServiceFactories;
import com.hazelcast.jet.pipeline.ServiceFactory;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.Sources;
import io.grpc.ManagedChannelBuilder;
import org.hazelcast.eventsourcing.EventSourcingController;
import org.hazelcast.eventsourcing.sync.CompletionInfo;
import org.hazelcast.msfdemo.invsvc.events.InventoryGrpc;
import org.hazelcast.msfdemo.invsvc.events.InventoryOuterClass;
import org.hazelcast.msfdemo.ordersvc.configuration.ServiceConfig;
import org.hazelcast.msfdemo.ordersvc.domain.Order;
import org.hazelcast.msfdemo.ordersvc.events.CreditCheckEvent;
import org.hazelcast.msfdemo.ordersvc.events.OrderEvent;
import org.hazelcast.msfdemo.ordersvc.events.PullInventoryEvent;
import org.hazelcast.msfdemo.ordersvc.events.ReserveInventoryEvent;
import org.hazelcast.msfdemo.ordersvc.service.OrderService;

import java.net.URL;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.logging.Logger;

import static com.hazelcast.jet.grpc.GrpcServices.unaryService;

public class PullInventoryPipeline implements Runnable {

    private static OrderService service;
    private List<URL> dependencies;
    private static String inventoryServiceHost;
    private static int inventoryServicePort;
    private static final Logger logger = Logger.getLogger(PullInventoryPipeline.class.getName());

    public PullInventoryPipeline(OrderService service, byte[] clientConfig, List<URL> dependentJars) {
        PullInventoryPipeline.service = service;
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
            logger.info("PullInventoryPipeline.run() invoked, submitting job");
            HazelcastInstance hazelcast = service.getHazelcastInstance();
            JobConfig jobConfig = new JobConfig();
            jobConfig.setName("OrderService.PullInventory");
            for (URL url : dependencies)
                jobConfig.addJar(url);
            hazelcast.getJet().newJob(createPipeline(), jobConfig);
        } catch (Exception e) { // Happens if our pipeline is not valid
            e.printStackTrace();
        }
    }

    private static Pipeline createPipeline() {
        Pipeline p = Pipeline.create();

        ServiceFactory<?, ? extends GrpcService<InventoryOuterClass.PullRequest, InventoryOuterClass.PullResponse>>
                inventoryService = unaryService(
                () -> ManagedChannelBuilder.forAddress(inventoryServiceHost, inventoryServicePort).usePlaintext(),
                channel -> InventoryGrpc.newStub(channel)::pull
        );

        // EventSourcingController will add event to event store, update the in-memory
        // materialized view, and publish the event to all subscribers
        ServiceFactory<?, EventSourcingController<Order,String,OrderEvent>> eventController =
                ServiceFactories.sharedService(
                        (ctx) -> service.getEventSourcingController()
                );

        // Read Combined CreditCheck + ReserveInventory events from MapJournal
        p.readFrom(Sources.<String, Tuple2<CreditCheckEvent,ReserveInventoryEvent>>mapJournal("JRN.CCandIRCombo", JournalInitialPosition.START_FROM_OLDEST))
                .withoutTimestamps()
                .setName("Read combined CreditCheck + InventoryReserve items from Map Journal")

                // Pull requested inventory
                // Invoke the pull service via gRPC
                .mapUsingServiceAsync(inventoryService, (service, entry) -> {
                    logger.info("PullInventoryPipeline building request");
                    CreditCheckEvent creditCheckEvent = entry.getValue().f0();
                    ReserveInventoryEvent reserveEvent = entry.getValue().f1();
                    //Order orderDO = reserveEvent.getOrderNumber()
                    String orderNumber = creditCheckEvent.getKey();
                    String itemNumber = reserveEvent.getItemNumber();
                    String location = reserveEvent.getLocation();
                    int quantity = reserveEvent.getQuantity();
                    String acctNumber = reserveEvent.getAccountNumber();
                    InventoryOuterClass.PullRequest request = InventoryOuterClass.PullRequest.newBuilder()
                            .setItemNumber(itemNumber)
                            .setLocation(location)
                            .setQuantity(quantity)
                            .build();

                    // Invoke the gRPC service asynchronously
                    return service.call(request)
                            // Create the CreditCheckEvent.
                            .thenApply(response -> {
                                boolean success = response.getSuccess();
                                PullInventoryEvent pullInventoryEvent = new PullInventoryEvent(orderNumber,
                                        acctNumber,
                                        itemNumber,
                                        location,
                                        quantity);
                                if (!success) {
                                    String failureReason = response.getReason();
                                    pullInventoryEvent.setFailureReason(failureReason);
                                }
                                return pullInventoryEvent;
                            });
                }).setName("Invoke PullInventory on InventoryService")

                // Call HandleEvent (append event, update materialized view, publish event)
                .mapUsingServiceAsync(eventController, (controller, pullInventoryEvent) -> {
                    CompletableFuture<CompletionInfo> completion = controller.handleEvent(pullInventoryEvent, UUID.randomUUID());
                    return completion;
                }).setName("Invoke EventSourcingController.handleEvent")

                // Write event to map where it will trigger subsequent pipeline stage
                .writeTo(Sinks.map("PullInventoryEvents",
// When CompletionInfo has event as a SourcedEvent, use this version
                        completionInfo -> completionInfo.getEvent().getKey(),
                        completionInfo -> completionInfo.getEvent()));
// When CompletionInfo has event as a GenericRecord, use this version:
//                        completionInfo -> {
//                            GenericRecord gr = (GenericRecord) completionInfo.getEvent();
//                            String key = gr.getString("key");
//                            return key;
//                        },
//                        completionInfo -> {
//                            GenericRecord gr = (GenericRecord) completionInfo.getEvent();
//                            logger.info("Sink PullInventoryEvent " + gr);
//                            return gr;
//                        })
//                );

        return p;
    }
}