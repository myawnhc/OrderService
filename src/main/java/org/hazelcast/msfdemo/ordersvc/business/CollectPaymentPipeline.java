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
import org.hazelcast.msfdemo.acctsvc.events.AccountGrpc;
import org.hazelcast.msfdemo.acctsvc.events.AccountOuterClass;
import org.hazelcast.msfdemo.ordersvc.configuration.ServiceConfig;
import org.hazelcast.msfdemo.ordersvc.domain.Order;
import org.hazelcast.msfdemo.ordersvc.events.CollectPaymentEvent;
import org.hazelcast.msfdemo.ordersvc.events.CreditCheckEvent;
import org.hazelcast.msfdemo.ordersvc.events.OrderEvent;
import org.hazelcast.msfdemo.ordersvc.events.ReserveInventoryEvent;
import org.hazelcast.msfdemo.ordersvc.service.OrderService;

import java.math.BigDecimal;
import java.net.URL;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.logging.Logger;

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

        // Read Combined CreditCheck + ReserveInventory events from MapJournal
        p.readFrom(Sources.<String, Tuple2<CreditCheckEvent,ReserveInventoryEvent>>mapJournal("JRN.CCandIRCombo", JournalInitialPosition.START_FROM_OLDEST))
                .withoutTimestamps()
                .setName("Read combined CreditCheck + InventoryReserve items from Map Journal")

                // Collect payment (AccountService.collectPayment)
                // Invoke the Account adjust balance service via gRPC
                .mapUsingServiceAsync(collectPaymentService, (service, entry) -> {
                    logger.info("CollectPaymentPipeline building request");
                    CreditCheckEvent creditCheckEvent = entry.getValue().f0();
                    // Not sure that we actually need the inventory event - but we should probably ensure that it was successful
                    ReserveInventoryEvent reserveEvent = entry.getValue().f1();
                    String orderNumber = creditCheckEvent.getOrderNumber();
                    String acctNumber = creditCheckEvent.accountNumber;
                    BigDecimal amount = creditCheckEvent.amountRequested;
                    int amountInCents = amount.movePointRight(2).intValue();
                    // TODO: do we reject non-approved requests here or are they filtered
                    //  out before they reach this point?

                    AccountOuterClass.AdjustBalanceRequest request = AccountOuterClass.AdjustBalanceRequest.newBuilder()
                            .setAccountNumber(acctNumber)
                            .setAmount(amountInCents)
                            .build();

                    // Invoke the gRPC service asynchronously
                    return service.call(request)
                            // Create the CollectPaymentEvent
                            .thenApply(response -> {
                                // we don't actually need the balance, just whether the payment succeeded
                                int balance = response.getNewBalance();
                                CollectPaymentEvent paymentEvent = new CollectPaymentEvent(orderNumber,
                                        acctNumber,
                                        amount);
                                return paymentEvent;
                            });
                }).setName("Invoke CollectPayment on AccountService")

                // Call HandleEvent (append event, update materialized view, publish event)
                .mapUsingServiceAsync(eventController, (controller, collectPaymentEvent) -> {
                    CompletableFuture<CompletionInfo> completion = controller.handleEvent(collectPaymentEvent, UUID.randomUUID());
                    return completion;
                }).setName("Invoke EventSourcingController.handleEvent")

                // Write event to map where it will trigger subsequent pipeline stage when combined
                .writeTo(Sinks.map("CollectPaymentEvents",
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
//                            logger.info("Sink CollectPaymentEvent " + gr);
//                            return gr;
//                        })
//                );

        return p;
    }
}