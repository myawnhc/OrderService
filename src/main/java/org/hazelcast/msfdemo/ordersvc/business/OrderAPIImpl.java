package org.hazelcast.msfdemo.ordersvc.business;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastJsonValue;
import com.hazelcast.map.IMap;
import com.hazelcast.org.json.JSONObject;
import io.grpc.stub.StreamObserver;
import org.example.grpc.APIBufferPair;
import org.example.grpc.Arity;
import org.hazelcast.eventsourcing.pubsub.Consumer;
import org.hazelcast.eventsourcing.pubsub.SubscriptionManager;
import org.hazelcast.eventsourcing.pubsub.impl.ReliableTopicSubMgr;
import org.hazelcast.msfdemo.ordersvc.clients.InventoryServiceClient;
import org.hazelcast.msfdemo.ordersvc.events.CreateOrderEvent;
import org.hazelcast.msfdemo.ordersvc.events.OrderEvent;
import org.hazelcast.msfdemo.ordersvc.events.OrderGrpc;
import org.hazelcast.msfdemo.ordersvc.events.OrderOuterClass;

import static org.hazelcast.msfdemo.ordersvc.events.OrderOuterClass.CreateOrderRequest;
import static org.hazelcast.msfdemo.ordersvc.events.OrderOuterClass.CreateOrderResponse;
import static org.hazelcast.msfdemo.ordersvc.events.OrderOuterClass.OrderCreated;
import static org.hazelcast.msfdemo.ordersvc.events.OrderOuterClass.SubscribeRequest;


import java.math.BigDecimal;
import java.util.UUID;
import java.util.logging.Logger;

public class OrderAPIImpl extends OrderGrpc.OrderImplBase {

    private static final Logger logger = Logger.getLogger(OrderAPIImpl.class.getName());
    //private OrderDAO accountDAO;
    private final IMap<String, APIBufferPair> bufferPairsForAPI;
    private final APIBufferPair<CreateOrderRequest,CreateOrderResponse> createOrderHandler;

    // Other services we communicate with
    private InventoryServiceClient inventoryServiceClient;

    // Each service manages its own subscription manager
    private final SubscriptionManager<OrderEvent> submgr;


    public OrderAPIImpl(HazelcastInstance hazelcast) {
        String serviceName = bindService().getServiceDescriptor().getName();
        logger.info("OrderAPIImpl initializing structures for " + serviceName);

        bufferPairsForAPI = hazelcast.getMap(serviceName+"_APIS");

        createOrderHandler = new APIBufferPair(hazelcast,"createOrder", Arity.UNARY, Arity.UNARY);
        bufferPairsForAPI.put("createOrder", createOrderHandler);

        inventoryServiceClient = new InventoryServiceClient();

        // TODO: continue adding buffer pairs for each new API

        //orderDAO = new OrderDAO(hazelcast);

        // Register all the events we're responsible for with the Subscription Manager
        submgr = new ReliableTopicSubMgr<>();
        SubscriptionManager.register(hazelcast, CreateOrderEvent.class, submgr);

    }

    @Override
    public void createOrder(CreateOrderRequest request, StreamObserver<CreateOrderResponse> responseObserver) {
        UUID identifier = UUID.randomUUID();
        createOrderHandler.putUnaryRequest(identifier, request);

        CreateOrderResponse response = createOrderHandler.getUnaryResponse(identifier);
        responseObserver.onNext(response);
        responseObserver.onCompleted();

        BigDecimal price = inventoryServiceClient.lookupPrice(request.getItemNumber());
        // Order.setExtendedPrice - if we just write to the DAO directly we have no
        // audit trail.
    }

    @Override
    public void subscribeToOrderCreated(SubscribeRequest request, StreamObserver<OrderCreated> responseObserver) {
        int fromOffset = 0; // Perhaps should add this to the request message?
        Consumer<OrderEvent> consumer = orderCreated -> {
            String orderNumber = orderCreated.getKey();
            HazelcastJsonValue payload = orderCreated.getPayload();
            JSONObject jobj = new JSONObject(payload.getValue());
            OrderCreated eventNotification = OrderCreated.newBuilder()
                    .setOrderNumber(orderNumber)
                    .setAccountNumber(jobj.getString(CreateOrderEvent.ACCT_NUM))
                    .setItemNumber(jobj.getString(CreateOrderEvent.ITEM_NUM))
                    .setLocation(jobj.getString(CreateOrderEvent.LOCATION))
                    .setQuantity(jobj.getInt(CreateOrderEvent.QUANTITY))
                    .build();
            responseObserver.onNext(eventNotification);
        };
        submgr.subscribe(CreateOrderEvent.class.getCanonicalName(), consumer, fromOffset);

    }
}
