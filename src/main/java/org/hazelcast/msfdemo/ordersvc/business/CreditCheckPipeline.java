package org.hazelcast.msfdemo.ordersvc.business;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.datamodel.Tuple2;
import com.hazelcast.jet.grpc.GrpcService;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.ServiceFactories;
import com.hazelcast.jet.pipeline.ServiceFactory;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.StreamStage;
import com.hazelcast.map.IMap;
import com.hazelcast.org.json.JSONObject;
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
import org.hazelcast.msfdemo.ordersvc.events.CreateOrderEvent;
import org.hazelcast.msfdemo.ordersvc.events.CreditCheckEvent;
import org.hazelcast.msfdemo.ordersvc.events.OrderEvent;
import org.hazelcast.msfdemo.ordersvc.events.OrderOuterClass;
import org.hazelcast.msfdemo.ordersvc.events.PriceLookupEvent;
import org.hazelcast.msfdemo.ordersvc.service.OrderService;

import java.net.URL;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.logging.Logger;

import static com.hazelcast.jet.datamodel.Tuple2.tuple2;
import static com.hazelcast.jet.grpc.GrpcServices.unaryService;

public class CreditCheckPipeline implements Runnable {

    private static OrderService service;
    private List<URL> dependencies;
    private static String creditCheckServiceHost;
    private static int creditCheckServicePort;
    private static final Logger logger = Logger.getLogger(CreditCheckPipeline.class.getName());

    public CreditCheckPipeline(OrderService service, byte[] clientConfig, List<URL> dependentJars) {
        CreditCheckPipeline.service = service;
        if (service == null)
            throw new IllegalArgumentException("Service cannot be null");
        // When running in client/server mode, service won't be initialized yet
        if (service.getEventSourcingController() == null && clientConfig != null) {
            service.initService(clientConfig);
        }
        this.dependencies = dependentJars;

        // Foreign service configuration
        ServiceConfig.ServiceProperties props = ServiceConfig.get("account-service");
        creditCheckServiceHost = props.getGrpcHostname();
        creditCheckServicePort = props.getGrpcPort();
    }

    @Override
    public void run() {
        try {
            logger.info("CreditCheckPipeline.run() invoked, submitting job");
            HazelcastInstance hazelcast = service.getHazelcastInstance();
            JobConfig jobConfig = new JobConfig();
            jobConfig.setName("OrderService.CreditCheck");
            for (URL url : dependencies)
                jobConfig.addJar(url);
            hazelcast.getJet().newJob(createPipeline(), jobConfig);
        } catch (Exception e) { // Happens if our pipeline is not valid
            e.printStackTrace();
        }
    }

    private static Pipeline createPipeline() {
        Pipeline p = Pipeline.create();

        SubscriptionManager<PriceLookupEvent> eventSource = new IMapSubMgr<>();
        SubscriptionManager.register(service.getHazelcastInstance(), PriceLookupEvent.class,
                eventSource);

        ServiceFactory<?, ? extends GrpcService<AccountOuterClass.CheckBalanceRequest, AccountOuterClass.CheckBalanceResponse>>
                creditCheckService = unaryService(
                () -> ManagedChannelBuilder.forAddress(creditCheckServiceHost, creditCheckServicePort).usePlaintext(),
                channel -> AccountGrpc.newStub(channel)::checkBalance
        );

        // EventSourcingController will add event to event store, update the in-memory
        // materialized view, and publish the event to all subscribers
        ServiceFactory<?, EventSourcingController<Order,String,OrderEvent>> eventController =
                ServiceFactories.sharedService(
                        (ctx) -> service.getEventSourcingController()
                );

        // Read events from MapJournal
        p.readFrom(eventSource.getStreamSource("PriceLookupEvent"))
                .withoutTimestamps()
                .setName("Read PriceLookupEvents from Map Journal")

                // OrderPriced event doesn't contain account number, so must enrich stream
                .mapUsingIMap(service.getOrderView(),
                        /* keyFn */ entry -> entry.getValue().getKey(),
                        /* mapFn */ (entry, order) -> tuple2(entry.getValue(), order))
                .setName("Enrich PriceLookupEvent with Order materialized view")

                // Check customer's credit balance (AccountService.checkBalance)
                // Invoke the Account check balance service via gRPC
                .mapUsingServiceAsync(creditCheckService, (service, tuple2) -> {
                    PriceLookupEvent priceLookupEvent = tuple2.f0();
                    Order orderDO = tuple2.f1();
                    String orderNumber = priceLookupEvent.getKey();
                    AccountOuterClass.CheckBalanceRequest request = AccountOuterClass.CheckBalanceRequest.newBuilder()
                            .setAccountNumber(orderDO.getAcctNumber())
                            .build();

                    // Invoke the gRPC service asynchronously
                    return service.call(request)
                            // Create the CreditCheckEvent.
                            .thenApply(response -> {
                                int creditAllowed = response.getBalance();
                                int amountRequested = orderDO.getExtendedPrice() * orderDO.getQuantity();
                                boolean approved = creditAllowed >= amountRequested;
                                CreditCheckEvent creditCheck = new CreditCheckEvent(orderNumber,
                                        orderDO.getAcctNumber(),
                                        amountRequested,
                                        approved);

                                return creditCheck;
                            });
                }).setName("Invoke CreditCheck on AccountService")

                // Call HandleEvent (append event, update materialized view, publish event)
                .mapUsingServiceAsync(eventController, (controller, creditCheckEvent) -> {
                    CompletableFuture<CompletionInfo> completion = controller.handleEvent(creditCheckEvent, UUID.randomUUID());
                    return completion;
                }).setName("Invoke EventSourcingController.handleEvent")

                // Send response - possibly nop if we're just responding to event
                .writeTo(Sinks.noop());

        return p;
    }
}