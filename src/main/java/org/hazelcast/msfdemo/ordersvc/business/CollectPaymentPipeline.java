package org.hazelcast.msfdemo.ordersvc.business;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastJsonValue;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.grpc.GrpcService;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.ServiceFactories;
import com.hazelcast.jet.pipeline.ServiceFactory;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.org.json.JSONObject;
import io.grpc.ManagedChannelBuilder;
import org.hazelcast.eventsourcing.EventSourcingController;
import org.hazelcast.eventsourcing.pubsub.SubscriptionManager;
import org.hazelcast.eventsourcing.pubsub.impl.IMapSubMgr;
import org.hazelcast.eventsourcing.sync.CompletionInfo;
import org.hazelcast.msfdemo.acctsvc.events.AccountGrpc;
import org.hazelcast.msfdemo.acctsvc.events.AccountOuterClass;
import org.hazelcast.msfdemo.ordersvc.configuration.ServiceConfig;
import org.hazelcast.msfdemo.ordersvc.domain.Order;
import org.hazelcast.msfdemo.ordersvc.events.CreditCheckEvent;
import org.hazelcast.msfdemo.ordersvc.events.OrderEvent;
import org.hazelcast.msfdemo.ordersvc.events.PriceLookupEvent;
import org.hazelcast.msfdemo.ordersvc.service.OrderService;

import java.net.URL;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.logging.Logger;

import static com.hazelcast.jet.datamodel.Tuple2.tuple2;
import static com.hazelcast.jet.grpc.GrpcServices.unaryService;

public class CollectPaymentPipeline implements Runnable {

    private static OrderService service;
    private List<URL> dependencies;
    private static String collectPaymentServiceHost;
    private static int collectPaymentServicePort;
    private static final Logger logger = Logger.getLogger(CollectPaymentPipeline.class.getName());

    public CollectPaymentPipeline(OrderService service, byte[] clientConfig, List<URL> dependentJars) {
        CollectPaymentPipeline.service = service;
        if (service == null)
            throw new IllegalArgumentException("Service cannot be null");
        // When running in client/server mode, service won't be initialized yet
        if (service.getEventSourcingController() == null && clientConfig != null) {
            service.initService(clientConfig);
        }
        this.dependencies = dependentJars;

        // Foreign service configuration
        ServiceConfig.ServiceProperties props = ServiceConfig.get("account-service");
        collectPaymentServiceHost = props.getGrpcHostname();
        collectPaymentServicePort = props.getGrpcPort();
    }

    @Override
    public void run() {
        try {
            logger.info("CollectPaymentPipeline.run() invoked, submitting job");
            HazelcastInstance hazelcast = service.getHazelcastInstance();
            JobConfig jobConfig = new JobConfig();
            jobConfig.setName("OrderService.CollectPayment");
            for (URL url : dependencies)
                jobConfig.addJar(url);
            hazelcast.getJet().newJob(createPipeline(), jobConfig);
        } catch (Exception e) { // Happens if our pipeline is not valid
            e.printStackTrace();
        }
    }

    private static Pipeline createPipeline() {
        Pipeline p = Pipeline.create();

        SubscriptionManager<CreditCheckEvent> eventSource = new IMapSubMgr<>();
        SubscriptionManager.register(service.getHazelcastInstance(), CreditCheckEvent.class,
                eventSource);

        // TODO: do we subscribe to ReserveInventoryEvent?  (If no one subscribes, the
        //  event will not be published).

        ServiceFactory<?, ? extends GrpcService<AccountOuterClass.AdjustBalanceRequest, AccountOuterClass.AdjustBalanceResponse>>
                collectPaymentService = unaryService(
                () -> ManagedChannelBuilder.forAddress(collectPaymentServiceHost, collectPaymentServicePort).usePlaintext(),
                channel -> AccountGrpc.newStub(channel)::payment
        );

        // EventSourcingController will add event to event store, update the in-memory
        // materialized view, and publish the event to all subscribers
        ServiceFactory<?, EventSourcingController<Order,String,OrderEvent>> eventController =
                ServiceFactories.sharedService(
                        (ctx) -> service.getEventSourcingController()
                );

        // Read events from MapJournal
        p.readFrom(eventSource.getStreamSource("CheckBalanceEvent"))
                .withoutTimestamps()
                .setName("Read CheckBalanceEvents from Map Journal")

                .peek()

//                // CheckBalance event doesn't contain account number, so must enrich stream
//                .mapUsingIMap(service.getOrderView(),
//                        /* keyFn */ entry -> entry.getValue().getKey(),
//                        /* mapFn */ (entry, order) -> tuple2(entry.getValue(), order))
//                .setName("Enrich CheckBalanceEvent with Order materialized view")
//
//                .peek()

                // TODO: here we need to join with ReserveInventory result ...
                //  await completion of that request if necessary

                // Collect payment (AccountService.collectPayment)
                // Invoke the Account check balance service via gRPC
                .mapUsingServiceAsync(collectPaymentService, (service, entry) -> {
                    CreditCheckEvent creditCheckEvent = entry.getValue();
                    JSONObject jobj = new JSONObject(creditCheckEvent.getPayload().getValue());
                    String acctNumber = jobj.getString(CreditCheckEvent.ACCT_NUMBER);
                    int amount = jobj.getInt(CreditCheckEvent.AMT_REQUESTED);
                    // TODO: do we reject non-approved requests here or are they filtered
                    //  out before they reach this point?

                    AccountOuterClass.AdjustBalanceRequest request = AccountOuterClass.AdjustBalanceRequest.newBuilder()
                            .setAccountNumber(acctNumber)
                            .setAmount(amount)
                            .build();

                    // Invoke the gRPC service asynchronously
                    return service.call(request)
                            // Create the CreditCheckEvent.
                            .thenApply(response -> {
                                // we don't need this info here
                                int balance = response.getNewBalance();
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