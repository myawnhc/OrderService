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
import com.hazelcast.org.json.JSONObject;
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
import org.hazelcast.msfdemo.ordersvc.events.PullInventoryEvent;
import org.hazelcast.msfdemo.ordersvc.events.ReserveInventoryEvent;
import org.hazelcast.msfdemo.ordersvc.events.ShipOrderEvent;
import org.hazelcast.msfdemo.ordersvc.service.OrderService;

import java.net.URL;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.logging.Logger;

import static com.hazelcast.jet.grpc.GrpcServices.unaryService;

public class ShipmentPipeline implements Runnable {

    private static OrderService service;
    private List<URL> dependencies;

    private static final Logger logger = Logger.getLogger(ShipmentPipeline.class.getName());

    public ShipmentPipeline(OrderService service, byte[] clientConfig, List<URL> dependentJars) {
        ShipmentPipeline.service = service;
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
            logger.info("ShipmentPipeline.run() invoked, submitting job");
            HazelcastInstance hazelcast = service.getHazelcastInstance();
            JobConfig jobConfig = new JobConfig();
            jobConfig.setName("OrderService.Shipment");
            for (URL url : dependencies)
                jobConfig.addJar(url);
            hazelcast.getJet().newJob(createPipeline(), jobConfig);
        } catch (Exception e) { // Happens if our pipeline is not valid
            e.printStackTrace();
        }
    }

    private static Pipeline createPipeline() {
        Pipeline p = Pipeline.create();

        // EventSourcingController will add event to event store, update the in-memory
        // materialized view, and publish the event to all subscribers
        ServiceFactory<?, EventSourcingController<Order,String,OrderEvent>> eventController =
                ServiceFactories.sharedService(
                        (ctx) -> service.getEventSourcingController()
                );

        // Read Combined CreditCheck + ReserveInventory events from MapJournal
        p.readFrom(Sources.<String, Tuple2<CollectPaymentEvent, PullInventoryEvent>>mapJournal("JRN.CPandPICombo", JournalInitialPosition.START_FROM_OLDEST))
                .withoutTimestamps()
                .setName("Read combined CollectPayment + PullInventory items from Map Journal")

                // No foreign service invocation required here
                .map(entry -> {
                    CollectPaymentEvent collectPaymentEvent = entry.getValue().f0();
                    PullInventoryEvent pullInventoryEvent = entry.getValue().f1();
                    String orderNumber = pullInventoryEvent.getOrderNumber();
                    JSONObject jobj = new JSONObject(pullInventoryEvent.getPayload().getValue());
                    String itemNumber = jobj.getString(PullInventoryEvent.ITEM_NUMBER);
                    String location = jobj.getString(PullInventoryEvent.LOCATION);
                    int quantity = jobj.getInt(PullInventoryEvent.QUANTITY);
                    ShipOrderEvent shipment = new ShipOrderEvent(orderNumber, itemNumber, location, quantity);
                    return shipment;
                })

                // Call HandleEvent (append event, update materialized view, publish event)
                .mapUsingServiceAsync(eventController, (controller, shipmentEvent) -> {
                    CompletableFuture<CompletionInfo> completion = controller.handleEvent(shipmentEvent, UUID.randomUUID());
                    return completion;
                }).setName("Invoke EventSourcingController.handleEvent")

                // Nothing required
                .writeTo(Sinks.noop());

        return p;
    }
}