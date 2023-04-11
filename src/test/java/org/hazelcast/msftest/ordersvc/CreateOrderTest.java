package org.hazelcast.msftest.ordersvc;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;

import org.hazelcast.msfdemo.ordersvc.configuration.ServiceConfig;
import org.hazelcast.msfdemo.ordersvc.events.OrderGrpc;
import org.hazelcast.msfdemo.ordersvc.events.OrderOuterClass;
import org.hazelcast.msfdemo.ordersvc.service.OrderService;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.Socket;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

public class CreateOrderTest {

    private static OrderGrpc.OrderBlockingStub blockingStub;
    private static OrderGrpc.OrderFutureStub futureStub;
    private static OrderGrpc.OrderStub asyncStub;

    private static String targetHost;
    private static int targetPort;
    private static final Logger logger = Logger.getLogger(CreateOrderTest.class.getName());

    @BeforeAll
    static void init() {
        try {
            // OrderService will start Hazelcast and GrpcServer
            Executors.newSingleThreadExecutor().submit( () -> {
                try {
                    OrderService.main(new String[]{});
                } catch (IOException | InterruptedException e) {
                    throw new RuntimeException(e);
                }
            });

            ServiceConfig.ServiceProperties props = ServiceConfig.get("order-service");
            String target = props.getTarget();
            logger.info("Target from service.yaml " + target);
            // Used to detect when service is ready to respond
            targetHost = props.getGrpcHostname();
            targetPort = props.getGrpcPort();

            ManagedChannel channel = ManagedChannelBuilder.forTarget(target)
                    .usePlaintext()
                    .build();
            blockingStub = OrderGrpc.newBlockingStub(channel);
            futureStub = OrderGrpc.newFutureStub(channel);
            asyncStub = OrderGrpc.newStub(channel);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @AfterAll
    static void cleanUp() {
//        hazelcast.shutdown();
    }

    @BeforeEach
    void setUp() {}

    @AfterEach
    void tearDown() {}

    @Test
    void verifyOrderCreation() throws ExecutionException, InterruptedException {
        OrderOuterClass.SubscribeRequest subscribeRequest = OrderOuterClass.SubscribeRequest.newBuilder().build();
        StreamObserver<OrderOuterClass.OrderCreated> observer = new StreamObserver<OrderOuterClass.OrderCreated>() {
            @Override
            public void onNext(OrderOuterClass.OrderCreated orderCreated) {
                logger.info("Subscribed notification: " + orderCreated);
                // TODO: have asserts to confirm correctness
            }

            @Override
            public void onError(Throwable throwable) {
                // TODO: fail
            }

            @Override
            public void onCompleted() {
                // nop
            }
        };
        logger.info("Making async call to subscription manager");
        try {
            boolean notReady = true;
            while (notReady) {
                try (Socket ignored = new Socket(targetHost, targetPort)) {
                    break;
                } catch (IOException e) {}
                try {
                    logger.info("Waiting on " + targetHost + ":" + targetPort + " to be ready");
                    Thread.sleep(1000);
                } catch (InterruptedException e) {}
            }
            asyncStub.subscribeToOrderCreated(subscribeRequest, observer);
        } catch (StatusRuntimeException sre) {
            sre.printStackTrace();
        }

        // Generally we wouldn't subscribe to notifications if we're suing the blocking
        // API, but in this case we want to test both at the same time.
            OrderOuterClass.CreateOrderRequest newOrder = OrderOuterClass.CreateOrderRequest.newBuilder()
                .setAccountNumber("Acct_42")
                .setItemNumber("Item_10101")
                .setLocation("Area51")
                .setQuantity(10)
                .build();

        logger.info("Making blocking call to createOrder");
        OrderOuterClass.CreateOrderResponse response;
        while (true) {
            try {
                response = blockingStub.createOrder(newOrder);
                //logger.info("** SERVER READY **");
                if (response == null)
                    Assertions.fail("Null response to CreateOrder call");
                logger.info("Order response from stub: " + response.getOrderNumber());
                Assertions.assertNotNull(response.getOrderNumber());
                break;
            } catch (StatusRuntimeException sre) {
                logger.info("Error on blockingStub call because server not ready, will wait and retry");
                Thread.sleep(1000);
            }
        }

        // how to handle multiple concurrent calls ...
        if (false) {
            ListenableFuture<OrderOuterClass.CreateOrderResponse> r1 = futureStub.createOrder(newOrder);
            ListenableFuture multiR = Futures.allAsList(r1); // add r2 as well
            multiR.get(); // responses will have different types ..
            // might addListener rather than get, can handle success and failure that way
        }
    }
}
