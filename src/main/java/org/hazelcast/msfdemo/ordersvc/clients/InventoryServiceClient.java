/*
 * Copyright 2018-2022 Hazelcast, Inc
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.hazelcast.msfdemo.ordersvc.clients;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.hazelcast.msfdemo.invsvc.events.InventoryGrpc;
import org.hazelcast.msfdemo.invsvc.events.InventoryOuterClass;
import org.hazelcast.msfdemo.ordersvc.configuration.ServiceConfig;

import java.math.BigDecimal;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import static org.hazelcast.msfdemo.invsvc.events.InventoryGrpc.*;
import static org.hazelcast.msfdemo.invsvc.events.InventoryOuterClass.*;


/** Will use this if we call InventoryService APIs directly via stub;
 *  but if we are just using event notifications to trigger responses then we don't
 *  need a client at all.
 */
public class InventoryServiceClient {
    private static final Logger logger = Logger.getLogger(InventoryServiceClient.class.getName());
    private InventoryBlockingStub blockingStub;
    private ManagedChannel channel;

    public InventoryServiceClient() {
        initChannel();
    }


    public ManagedChannel initChannel() {

        ServiceConfig.ServiceProperties props = ServiceConfig.get("inventory-service");
        String target = props.getTarget();
        logger.info("Target from service.yaml " + target);

        ManagedChannel channel = ManagedChannelBuilder.forTarget(target)
                .usePlaintext()
                .build();

        blockingStub = newBlockingStub(channel);

        return channel;
    }

    private void shutdownChannel() {
        try {
            channel.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            ;
        }
    }

    // Initially thinking we using blocking API here, can revisit this decision later
    public BigDecimal lookupPrice(String itemNumber) {
        PriceLookupRequest request = PriceLookupRequest.newBuilder()
                .setItemNumber(itemNumber)
                .build();
        PriceLookupResponse response = blockingStub.priceLookup(request);
        int priceInCents = response.getPrice();
        BigDecimal price = BigDecimal.valueOf(priceInCents).movePointLeft(2);
        return price;
    }

//e    public int getItemCount() {
//        if (blockingStub == null)
//            initChannel();
//
//        InventoryOuterClass.ItemCountRequest request = InventoryOuterClass.ItemCountRequest.newBuilder()
//                .build();
//
//        InventoryOuterClass.ItemCountResponse response = blockingStub.getItemCount(request);
//        return response.getCount();
//    }
//
//    public int getInventoryRecordCount() {
//        if (blockingStub == null)
//            initChannel();
//
//        InventoryOuterClass.InventoryCountRequest request = InventoryOuterClass.InventoryCountRequest.newBuilder()
//                .build();
//
//        InventoryOuterClass.InventoryCountResponse response = blockingStub.getInventoryRecordCount(request);
//        return response.getCount();
//    }
//
//    // NEW
//    public boolean reserveInventory(String item, String location, int qty) {
//        if (blockingStub == null)
//            initChannel();
//
//        InventoryOuterClass.ReserveRequest request = InventoryOuterClass.ReserveRequest.newBuilder()
//                .setItemNumber(item)
//                .setLocation(location)
//                .setQuantity(qty).build();
//
//        InventoryOuterClass.ReserveResponse response = blockingStub.reserve(request);
//        if (!response.getSuccess())
//            System.out.println("ReserveInventory failed: " + response.getReason());
//
//        return response.getSuccess();
//    }
//
//    public boolean pullInventory(String item, String location, int qty) {
//        if (blockingStub == null)
//            initChannel();
//
//        InventoryOuterClass.PullRequest request = InventoryOuterClass.PullRequest.newBuilder()
//                .setItemNumber(item)
//                .setLocation(location)
//                .setQuantity(qty).build();
//
//        InventoryOuterClass.PullResponse response = blockingStub.pull(request);
//        if (!response.getSuccess())
//            System.out.println("PullInventory failed: " + response.getReason());
//
//        return response.getSuccess();
//    }
}
