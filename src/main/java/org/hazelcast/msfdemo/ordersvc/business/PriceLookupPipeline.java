package org.hazelcast.msfdemo.ordersvc.business;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.core.metrics.Metrics;
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
import org.hazelcast.msfdemo.invsvc.events.InventoryGrpc;
import org.hazelcast.msfdemo.invsvc.events.InventoryOuterClass;
import org.hazelcast.msfdemo.ordersvc.configuration.ServiceConfig;
import org.hazelcast.msfdemo.ordersvc.domain.Order;
import org.hazelcast.msfdemo.ordersvc.events.CreateOrderEvent;
import org.hazelcast.msfdemo.ordersvc.events.OrderEvent;
import org.hazelcast.msfdemo.ordersvc.events.PriceLookupEvent;
import org.hazelcast.msfdemo.ordersvc.service.OrderService;

import java.math.BigDecimal;
import java.net.URL;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.logging.Logger;

import static com.hazelcast.jet.datamodel.Tuple2.tuple2;
import static com.hazelcast.jet.grpc.GrpcServices.unaryService;

public class PriceLookupPipeline implements Runnable {

    private static OrderService service;
    private List<URL> dependencies;
    private static String priceLookupServiceHost;
    private static int priceLookupServicePort;
    private static final Logger logger = Logger.getLogger(PriceLookupPipeline.class.getName());

    public PriceLookupPipeline(OrderService service, byte[] clientConfig, List<URL> dependentJars) {
        PriceLookupPipeline.service = service;
        if (service == null)
            throw new IllegalArgumentException("Service cannot be null");
        // When running in client/server mode, service won't be initialized yet
        if (service.getEventSourcingController() == null && clientConfig != null) {
            service.initService(clientConfig);
        }
        this.dependencies = dependentJars;

        // Foreign service configuration
        ServiceConfig.ServiceProperties props = ServiceConfig.get("inventory-service");
        priceLookupServiceHost = props.getGrpcHostname();
        priceLookupServicePort = props.getGrpcPort();
    }

    @Override
    public void run() {
        try {
            logger.info("PriceLookupPipeline.run() invoked, submitting job");
            HazelcastInstance hazelcast = service.getHazelcastInstance();
            JobConfig jobConfig = new JobConfig();
            jobConfig.setName("OrderService.PriceLookup");
            for (URL url : dependencies)
                jobConfig.addJar(url);
            hazelcast.getJet().newJob(createPipeline(), jobConfig);
        } catch (Exception e) { // Happens if our pipeline is not valid
            e.printStackTrace();
        }
    }

    private static Pipeline createPipeline() {
        Pipeline p = Pipeline.create();

        SubscriptionManager<CreateOrderEvent> eventSource = new IMapSubMgr<>("OrderEvent");
        SubscriptionManager.register(service.getHazelcastInstance(), CreateOrderEvent.QUAL_EVENT_NAME,
                eventSource);
        // With getStreamSource, do not need to actually call isubmgr.subscribe ...
        // Guess there is no logical place to unregister since pipeline will run as long
        //   as the VM is up

        ServiceFactory<?, ? extends GrpcService<InventoryOuterClass.PriceLookupRequest, InventoryOuterClass.PriceLookupResponse>>
                priceLookupService = unaryService(
                () -> ManagedChannelBuilder.forAddress(priceLookupServiceHost, priceLookupServicePort).usePlaintext(),
                channel -> InventoryGrpc.newStub(channel)::priceLookup
        );

        // EventSourcingController will add event to event store, update the in-memory
        // materialized view, and publish the event to all subscribers
        ServiceFactory<?, EventSourcingController<Order,String,OrderEvent>> eventController =
                ServiceFactories.sharedService(
                        (ctx) -> service.getEventSourcingController()
                );

        // Stage 0: Read events from MapJournal
        p.readFrom(eventSource.getStreamSource(CreateOrderEvent.QUAL_EVENT_NAME))
                .withoutTimestamps()
                .setName("Read CreateOrderEvents from Map Journal")

        // Stage 1: Lookup the price (InventoryService.priceLookup)
                // Invoke the Inventory price lookup service via gRPC
                .mapUsingServiceAsync(priceLookupService, (service, eventEntry) -> {
                    //OK logger.info("PriceLookupPipeline building request");
                    CreateOrderEvent orderCreated = eventEntry.getValue();
                    String orderNumber = orderCreated.getKey();
                    String itemNumber = orderCreated.getItemNumber();
                    String location = orderCreated.getLocation();
                    int quantity = orderCreated.getQuantity();
                    InventoryOuterClass.PriceLookupRequest request = InventoryOuterClass.PriceLookupRequest.newBuilder()
                            .setItemNumber(itemNumber)
                            .build();
                    //logger.info("PriceLookupPipeline calling InventoryService::PriceLookup via gRPC");

                    return service.call(request)
                            // Stage 2: Create the PriceLookupEvent
                            .thenApply(response -> {
                                //OK logger.info("PriceLookupPipeline received PriceLookup response, creating event");
                                int priceInCents = response.getPrice();
                                BigDecimal price = BigDecimal.valueOf(priceInCents).movePointLeft(2);
                                PriceLookupEvent lookup = new PriceLookupEvent(orderNumber,
                                        itemNumber, location, quantity, price);
                                return tuple2(eventEntry.getKey(), lookup);
                            });
                }).setName("Invoke PriceLookup on InventoryService")

        // Stage 3: Call HandleEvent (append event, update materialized view, publish event)
                .mapUsingServiceAsync(eventController, (controller, tuple) -> {
                    //OK logger.info("PriceLookupPipeline calling handleEvent for PriceLookupEvent");
                    CompletableFuture<CompletionInfo> completion = controller.handleEvent(tuple.f1(), UUID.randomUUID());
                    //OK logger.info("PriceLookupPipeline final step - future returned for order " + tuple.f1().getOrderNumber());
                    Metrics.metric("PLP.completions").increment();
                    return completion;
                }).setName("Invoke EventSourcingController.handleEvent")

        // Stage 4: Send response - possibly nop if we're just responding to event
                .writeTo(Sinks.noop());

        return p;
    }
}