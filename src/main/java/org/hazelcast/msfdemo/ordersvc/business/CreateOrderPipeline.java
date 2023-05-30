package org.hazelcast.msfdemo.ordersvc.business;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.crdt.pncounter.PNCounter;
import com.hazelcast.flakeidgen.FlakeIdGenerator;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.datamodel.Tuple2;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.ServiceFactories;
import com.hazelcast.jet.pipeline.ServiceFactory;
import com.hazelcast.jet.pipeline.StreamStage;
import org.example.grpc.GrpcConnector;
import org.example.grpc.MessageWithUUID;
import org.hazelcast.eventsourcing.EventSourcingController;
import org.hazelcast.eventsourcing.sync.CompletionInfo;
import org.hazelcast.msfdemo.ordersvc.dashboard.CounterService;
import org.hazelcast.msfdemo.ordersvc.domain.Order;
import org.hazelcast.msfdemo.ordersvc.events.CreateOrderEvent;
import org.hazelcast.msfdemo.ordersvc.events.OrderEvent;
import org.hazelcast.msfdemo.ordersvc.events.OrderOuterClass;
import org.hazelcast.msfdemo.ordersvc.service.OrderService;

import static org.hazelcast.msfdemo.ordersvc.events.OrderOuterClass.CreateOrderRequest;
import static org.hazelcast.msfdemo.ordersvc.events.OrderOuterClass.CreateOrderResponse;

import java.math.BigDecimal;
import java.net.URL;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.logging.Logger;

import static com.hazelcast.jet.datamodel.Tuple2.tuple2;

public class CreateOrderPipeline implements Runnable {

    private static OrderService service;
    private List<URL> dependencies;
    private static final Logger logger = Logger.getLogger(CreateOrderPipeline.class.getName());

//    // Metrics moved to CounterService class
//    public final static String PNC_CREATEORDER_IN = "CreateOrder_IN";
//    public final static String PNC_CREATEORDER_OK = "CreateOrder_OK";
//    public final static String PNC_CREATEORDER_FAIL = "CreateOrder_FAIL";
//    public final static String PNC_CREATEORDER_COMPLETE = "CreateOrder_COMPLETE";
//    public final static String PNC_CREATEORDER_ELAPSED_OK = "CreateOrder_ELAPSED_OK";
//    public final static String PNC_CREATEORDER_ELAPSED_FAIL = "CreateOrder_ELAPSED_FAIL";


    public CreateOrderPipeline(OrderService service, byte[] clientConfig, List<URL> dependentJars) {
        CreateOrderPipeline.service = service;
        if (service == null)
            throw new IllegalArgumentException("Service cannot be null");
        // When running in client/server mode, service won't be initialized yet
        if (service.getEventSourcingController() == null && clientConfig != null) {
            service.initService(clientConfig);
        }
        this.dependencies = dependentJars;
    }

    @Override
    public void run() {
        try {
            logger.info("CreateOrderPipeline.run() invoked, submitting job");
            HazelcastInstance hazelcast = service.getHazelcastInstance();
            JobConfig jobConfig = new JobConfig();
            jobConfig.setName("OrderService.CreateOrder");
            for (URL url : dependencies)
                jobConfig.addJar(url);
            hazelcast.getJet().newJob(createPipeline(), jobConfig);
        } catch (Exception e) { // Happens if our pipeline is not valid
            e.printStackTrace();
        }
    }

    private static Pipeline createPipeline() {
        Pipeline p = Pipeline.create();

        final String SERVICE_NAME = "order.Order";
        final String METHOD_NAME = "createOrder";

        StreamStage<MessageWithUUID<CreateOrderRequest>> requests =
                p.readFrom(GrpcConnector.<CreateOrderRequest>grpcUnarySource(SERVICE_NAME, METHOD_NAME))
                        .withoutTimestamps()
                        .setName("Read CreateOrder requests from GrpcSource");


        // Metrics collection
        ServiceFactory<?, CounterService> counterServiceFactory =
                ServiceFactories.sharedService(
                        (ctx) -> new CounterService(ctx.hazelcastInstance(), "CreateOrder")
                );


        // Update IN counter and start timer for this UUID + Pipeline Stage
        StreamStage<MessageWithUUID<CreateOrderRequest>> r2 = requests.mapUsingService(counterServiceFactory, (svc, entry) -> {
            svc.getInCounter().incrementAndGet();
            svc.startTimer(entry.getIdentifier());
            return entry;
        });

        // Flake ID Generator is used to generated order numbers for newly created orders
        ServiceFactory<?, FlakeIdGenerator> sequenceGeneratorServiceFactory =
                ServiceFactories.sharedService(
                        (ctx) -> {
                            HazelcastInstance hz = ctx.hazelcastInstance();
                            return hz.getFlakeIdGenerator("orderNumber");
                        }
                );

        // Create the CreateOrderEvent object and assign generated order number
        StreamStage<Tuple2<UUID, CreateOrderEvent>> events =
                r2.mapUsingService(sequenceGeneratorServiceFactory, (seqGen, entry) -> {
                            UUID uniqueRequestID = entry.getIdentifier();
                            CreateOrderRequest request = entry.getMessage();
                            long orderID = seqGen.newId();
                            CreateOrderEvent event = new CreateOrderEvent(
                                    ""+orderID,
                                    request.getAccountNumber(),
                                    request.getItemNumber(),
                                    request.getLocation(),
                                    request.getQuantity());
                            Tuple2<UUID,CreateOrderEvent> item = tuple2(uniqueRequestID, event);
                            return item;
                        })
                        .setName("Generate Order # and create CreateOrderEvent");

        // EventSourcingController will add event to event store, update the in-memory
        // materialized view, and publish the event to all subscribers
        ServiceFactory<?, EventSourcingController<Order,String,OrderEvent>> eventController =
                ServiceFactories.sharedService(
                        // In C/S config, service is uninitialized!
                        (ctx) -> {
                            if (service == null) {
                                System.out.println("**** ESC service factory needs to initialize service");
                                Map<String,Object> configMap = (Map<String, Object>) ctx.hazelcastInstance().getMap("ServiceConfig").get("AccountService");
                                byte[] clientConfig = (byte[]) configMap.get("clientConfig");
                                service = new OrderService();
                                service.initService(clientConfig);
                                System.out.println("service initialized from configmap");
                            }
                            return service.getEventSourcingController();
                        }).toNonCooperative(); // Experimental change

        StreamStage<MessageWithUUID<CreateOrderResponse>> message = events.mapUsingServiceAsync(eventController, (controller, tuple) -> {
                            CompletableFuture<CompletionInfo> completion = controller.handleEvent(tuple.f1(), tuple.f0());
                            return completion;
                        }).setName("Invoke EventSourcingController.handleEvent")

                // Send response back via GrpcSink
                .map(completion -> {
                    UUID uuid = completion.getUUID();
                    CreateOrderEvent event = (CreateOrderEvent) completion.getEvent();
                    String orderNumber = event.getKey();
                    CreateOrderResponse response = CreateOrderResponse.newBuilder()
                            .setOrderNumber(orderNumber)
                            .build();
                    MessageWithUUID<CreateOrderResponse> wrapped = new MessageWithUUID<>(uuid, response);
                    return wrapped;
                }).setName("Build CreateOrderResponse");

        // Increment counter of processed items, stop elapsed timer for the unique identifier
        StreamStage<MessageWithUUID<CreateOrderResponse>> m2 = message.mapUsingService(counterServiceFactory, (svc, msg) -> {
            svc.getOKCounter().incrementAndGet();
            svc.getCompletedCounter().incrementAndGet();
            svc.stopTime(msg.getIdentifier());
            return msg;
        });

        // Send the gRPC response
        m2.writeTo(GrpcConnector.grpcUnarySink(SERVICE_NAME, METHOD_NAME))
                .setName("Write response to GrpcSink");

        return p;
    }
}
