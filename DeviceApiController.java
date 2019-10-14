/**
 * Copyright © 2016-2019 The Thingsboard Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.thingsboard.server.transport.http;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.JsonElement;
import java.awt.event.ItemEvent;
import java.util.Map.Entry;
import com.google.gson.Gson; 
import com.google.gson.JsonArray;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.util.StringUtils;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.context.request.async.DeferredResult;
import org.thingsboard.server.common.data.id.DeviceId;
import org.thingsboard.server.common.transport.SessionMsgListener;
import org.thingsboard.server.common.transport.TransportContext;
import org.thingsboard.server.common.transport.TransportService;
import org.thingsboard.server.common.transport.TransportServiceCallback;
import org.thingsboard.server.common.transport.adaptor.JsonConverter;
import org.thingsboard.server.gen.transport.TransportProtos.AttributeUpdateNotificationMsg;
import org.thingsboard.server.gen.transport.TransportProtos.DeviceInfoProto;
import org.thingsboard.server.gen.transport.TransportProtos.GetAttributeRequestMsg;
import org.thingsboard.server.gen.transport.TransportProtos.GetAttributeResponseMsg;
import org.thingsboard.server.gen.transport.TransportProtos.SessionCloseNotificationProto;
import org.thingsboard.server.gen.transport.TransportProtos.SessionInfoProto;
import org.thingsboard.server.gen.transport.TransportProtos.SubscribeToAttributeUpdatesMsg;
import org.thingsboard.server.gen.transport.TransportProtos.SubscribeToRPCMsg;
import org.thingsboard.server.gen.transport.TransportProtos.ToDeviceRpcRequestMsg;
import org.thingsboard.server.gen.transport.TransportProtos.ToDeviceRpcResponseMsg;
import org.thingsboard.server.gen.transport.TransportProtos.ToServerRpcRequestMsg;
import org.thingsboard.server.gen.transport.TransportProtos.ToServerRpcResponseMsg;
import org.thingsboard.server.gen.transport.TransportProtos.ValidateDeviceCredentialsResponseMsg;
import org.thingsboard.server.gen.transport.TransportProtos.ValidateDeviceTokenRequestMsg;

import javax.servlet.http.HttpServletRequest;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.function.Consumer;

/**
 * @author Andrew Shvayka
 */
@RestController
@ConditionalOnExpression("'${transport.type:null}'=='null' || ('${transport.type}'=='local' && '${transport.http.enabled}'=='true')")
@RequestMapping("/api/v1")
@Slf4j
public class DeviceApiController {

    @Autowired
    private HttpTransportContext transportContext;

    @RequestMapping(value = "/{deviceToken}/attributes", method = RequestMethod.GET, produces = "application/json")
    public DeferredResult<ResponseEntity> getDeviceAttributes(@PathVariable("deviceToken") String deviceToken,
                                                              @RequestParam(value = "clientKeys", required = false, defaultValue = "") String clientKeys,
                                                              @RequestParam(value = "sharedKeys", required = false, defaultValue = "") String sharedKeys,
                                                              HttpServletRequest httpRequest) {
        DeferredResult<ResponseEntity> responseWriter = new DeferredResult<>();
        transportContext.getTransportService().process(ValidateDeviceTokenRequestMsg.newBuilder().setToken(deviceToken).build(),
                new DeviceAuthCallback(transportContext, responseWriter, sessionInfo -> {
                    GetAttributeRequestMsg.Builder request = GetAttributeRequestMsg.newBuilder().setRequestId(0);
                    List<String> clientKeySet = !StringUtils.isEmpty(clientKeys) ? Arrays.asList(clientKeys.split(",")) : null;
                    List<String> sharedKeySet = !StringUtils.isEmpty(sharedKeys) ? Arrays.asList(sharedKeys.split(",")) : null;
                    if (clientKeySet != null) {
                        request.addAllClientAttributeNames(clientKeySet);
                    }
                    if (sharedKeySet != null) {
                        request.addAllSharedAttributeNames(sharedKeySet);
                    }
                    TransportService transportService = transportContext.getTransportService();
                    transportService.registerSyncSession(sessionInfo, new HttpSessionListener(responseWriter), transportContext.getDefaultTimeout());
                    transportService.process(sessionInfo, request.build(), new SessionCloseOnErrorCallback(transportService, sessionInfo));
                }));
        return responseWriter;
    }

    @RequestMapping(value = "/{deviceToken}/attributes", method = RequestMethod.POST)
    public DeferredResult<ResponseEntity> postDeviceAttributes(@PathVariable("deviceToken") String deviceToken,
                                                               @RequestBody String json, HttpServletRequest request) {
        DeferredResult<ResponseEntity> responseWriter = new DeferredResult<>();
        transportContext.getTransportService().process(ValidateDeviceTokenRequestMsg.newBuilder().setToken(deviceToken).build(),
                new DeviceAuthCallback(transportContext, responseWriter, sessionInfo -> {
                    TransportService transportService = transportContext.getTransportService();
                    transportService.process(sessionInfo, JsonConverter.convertToAttributesProto(new JsonParser().parse(json)),
                            new HttpOkCallback(responseWriter));
                }));
        return responseWriter;
    }
public static JsonObject getJsonObjectFromArray(JsonObject data){
		JsonObject jobj = new JsonObject();
		for (Entry<String, JsonElement> valueEntry : data.entrySet()) {
            JsonElement element = valueEntry.getValue();         
            if (element.isJsonPrimitive()) {
            	jobj.add(valueEntry.getKey(),element);
            	
            } else {
            	JsonArray arr = new JsonArray();
            	arr = element.getAsJsonArray();
            	for(int i=0 ; i<arr.size();i++){
            		jobj.add(valueEntry.getKey() + i , arr.get(i) );
            	}
            }
		}
		return jobj;
		}
    @RequestMapping(value = "/{deviceToken}/telemetry", method = RequestMethod.POST)
    public DeferredResult<ResponseEntity> postTelemetry(@PathVariable("deviceToken") String deviceToken,
                                                        @RequestBody String json, HttpServletRequest request) {
        DeferredResult<ResponseEntity> responseWriter = new DeferredResult<ResponseEntity>();
		JsonElement jelem = new JsonParser().parse(json);
		JsonObject jsobj = jelem.getAsJsonObject();
		JsonArray arr = jsobj.get("data_list").getAsJsonArray();
		int len = arr.size();
		long start=System.currentTimeMillis();
		switch (len){
			case 1:
				transportContext.getTransportService().process(ValidateDeviceTokenRequestMsg.newBuilder().setToken(deviceToken).build(),
						new DeviceAuthCallback(transportContext, responseWriter, sessionInfo -> {
							TransportService transportService = transportContext.getTransportService();
							transportService.process(sessionInfo, JsonConverter.convertToTelemetryProto(start,getJsonObjectFromArray(arr.get(0).getAsJsonObject())),
									new HttpOkCallback(responseWriter));
						}));
				break;
			case 2:
				transportContext.getTransportService().process(ValidateDeviceTokenRequestMsg.newBuilder().setToken(deviceToken).build(),
						new DeviceAuthCallback(transportContext, responseWriter, sessionInfo -> {
							TransportService transportService = transportContext.getTransportService();
							transportService.process(sessionInfo, JsonConverter.convertToTelemetryProto(start+1,getJsonObjectFromArray(arr.get(1).getAsJsonObject())),
									new HttpOkCallback(responseWriter));
						}));
				transportContext.getTransportService().process(ValidateDeviceTokenRequestMsg.newBuilder().setToken(deviceToken).build(),
					new DeviceAuthCallback(transportContext, responseWriter, sessionInfo -> {
						TransportService transportService = transportContext.getTransportService();
						transportService.process(sessionInfo, JsonConverter.convertToTelemetryProto(start,getJsonObjectFromArray(arr.get(0).getAsJsonObject())),
								new HttpOkCallback(responseWriter));
					}));
				break;
			case 3:
				transportContext.getTransportService().process(ValidateDeviceTokenRequestMsg.newBuilder().setToken(deviceToken).build(),
						new DeviceAuthCallback(transportContext, responseWriter, sessionInfo -> {
							TransportService transportService = transportContext.getTransportService();
							transportService.process(sessionInfo, JsonConverter.convertToTelemetryProto(start+2,getJsonObjectFromArray(arr.get(2).getAsJsonObject())),
									new HttpOkCallback(responseWriter));
						}));
				transportContext.getTransportService().process(ValidateDeviceTokenRequestMsg.newBuilder().setToken(deviceToken).build(),
						new DeviceAuthCallback(transportContext, responseWriter, sessionInfo -> {
							TransportService transportService = transportContext.getTransportService();
							transportService.process(sessionInfo, JsonConverter.convertToTelemetryProto(start+1,getJsonObjectFromArray(arr.get(1).getAsJsonObject())),
									new HttpOkCallback(responseWriter));
						}));
				transportContext.getTransportService().process(ValidateDeviceTokenRequestMsg.newBuilder().setToken(deviceToken).build(),
					new DeviceAuthCallback(transportContext, responseWriter, sessionInfo -> {
						TransportService transportService = transportContext.getTransportService();
						transportService.process(sessionInfo, JsonConverter.convertToTelemetryProto(start,getJsonObjectFromArray(arr.get(0).getAsJsonObject())),
								new HttpOkCallback(responseWriter));
					}));
				break;
			case 4:
				transportContext.getTransportService().process(ValidateDeviceTokenRequestMsg.newBuilder().setToken(deviceToken).build(),
						new DeviceAuthCallback(transportContext, responseWriter, sessionInfo -> {
							TransportService transportService = transportContext.getTransportService();
							transportService.process(sessionInfo, JsonConverter.convertToTelemetryProto(start+3,getJsonObjectFromArray(arr.get(3).getAsJsonObject())),
									new HttpOkCallback(responseWriter));
						}));
				transportContext.getTransportService().process(ValidateDeviceTokenRequestMsg.newBuilder().setToken(deviceToken).build(),
						new DeviceAuthCallback(transportContext, responseWriter, sessionInfo -> {
							TransportService transportService = transportContext.getTransportService();
							transportService.process(sessionInfo, JsonConverter.convertToTelemetryProto(start+2,getJsonObjectFromArray(arr.get(2).getAsJsonObject())),
									new HttpOkCallback(responseWriter));
						}));
				transportContext.getTransportService().process(ValidateDeviceTokenRequestMsg.newBuilder().setToken(deviceToken).build(),
						new DeviceAuthCallback(transportContext, responseWriter, sessionInfo -> {
							TransportService transportService = transportContext.getTransportService();
							transportService.process(sessionInfo, JsonConverter.convertToTelemetryProto(start+1,getJsonObjectFromArray(arr.get(1).getAsJsonObject())),
									new HttpOkCallback(responseWriter));
						}));
				transportContext.getTransportService().process(ValidateDeviceTokenRequestMsg.newBuilder().setToken(deviceToken).build(),
					new DeviceAuthCallback(transportContext, responseWriter, sessionInfo -> {
						TransportService transportService = transportContext.getTransportService();
						transportService.process(sessionInfo, JsonConverter.convertToTelemetryProto(start,getJsonObjectFromArray(arr.get(0).getAsJsonObject())),
								new HttpOkCallback(responseWriter));
					}));
				break;
			case 5:
				transportContext.getTransportService().process(ValidateDeviceTokenRequestMsg.newBuilder().setToken(deviceToken).build(),
						new DeviceAuthCallback(transportContext, responseWriter, sessionInfo -> {
							TransportService transportService = transportContext.getTransportService();
							transportService.process(sessionInfo, JsonConverter.convertToTelemetryProto(start+4,getJsonObjectFromArray(arr.get(4).getAsJsonObject())),
									new HttpOkCallback(responseWriter));
						}));
				transportContext.getTransportService().process(ValidateDeviceTokenRequestMsg.newBuilder().setToken(deviceToken).build(),
						new DeviceAuthCallback(transportContext, responseWriter, sessionInfo -> {
							TransportService transportService = transportContext.getTransportService();
							transportService.process(sessionInfo, JsonConverter.convertToTelemetryProto(start+3,getJsonObjectFromArray(arr.get(3).getAsJsonObject())),
									new HttpOkCallback(responseWriter));
						}));
				transportContext.getTransportService().process(ValidateDeviceTokenRequestMsg.newBuilder().setToken(deviceToken).build(),
						new DeviceAuthCallback(transportContext, responseWriter, sessionInfo -> {
							TransportService transportService = transportContext.getTransportService();
							transportService.process(sessionInfo, JsonConverter.convertToTelemetryProto(start+2,getJsonObjectFromArray(arr.get(2).getAsJsonObject())),
									new HttpOkCallback(responseWriter));
						}));
				transportContext.getTransportService().process(ValidateDeviceTokenRequestMsg.newBuilder().setToken(deviceToken).build(),
						new DeviceAuthCallback(transportContext, responseWriter, sessionInfo -> {
							TransportService transportService = transportContext.getTransportService();
							transportService.process(sessionInfo, JsonConverter.convertToTelemetryProto(start+1,getJsonObjectFromArray(arr.get(1).getAsJsonObject())),
									new HttpOkCallback(responseWriter));
						}));
				transportContext.getTransportService().process(ValidateDeviceTokenRequestMsg.newBuilder().setToken(deviceToken).build(),
					new DeviceAuthCallback(transportContext, responseWriter, sessionInfo -> {
						TransportService transportService = transportContext.getTransportService();
						transportService.process(sessionInfo, JsonConverter.convertToTelemetryProto(start,getJsonObjectFromArray(arr.get(0).getAsJsonObject())),
								new HttpOkCallback(responseWriter));
					}));
				break;
			case 6:
				transportContext.getTransportService().process(ValidateDeviceTokenRequestMsg.newBuilder().setToken(deviceToken).build(),
						new DeviceAuthCallback(transportContext, responseWriter, sessionInfo -> {
							TransportService transportService = transportContext.getTransportService();
							transportService.process(sessionInfo, JsonConverter.convertToTelemetryProto(start+5,getJsonObjectFromArray(arr.get(5).getAsJsonObject())),
									new HttpOkCallback(responseWriter));
						}));
				transportContext.getTransportService().process(ValidateDeviceTokenRequestMsg.newBuilder().setToken(deviceToken).build(),
						new DeviceAuthCallback(transportContext, responseWriter, sessionInfo -> {
							TransportService transportService = transportContext.getTransportService();
							transportService.process(sessionInfo, JsonConverter.convertToTelemetryProto(start+4,getJsonObjectFromArray(arr.get(4).getAsJsonObject())),
									new HttpOkCallback(responseWriter));
						}));
				transportContext.getTransportService().process(ValidateDeviceTokenRequestMsg.newBuilder().setToken(deviceToken).build(),
						new DeviceAuthCallback(transportContext, responseWriter, sessionInfo -> {
							TransportService transportService = transportContext.getTransportService();
							transportService.process(sessionInfo, JsonConverter.convertToTelemetryProto(start+3,getJsonObjectFromArray(arr.get(3).getAsJsonObject())),
									new HttpOkCallback(responseWriter));
						}));
				transportContext.getTransportService().process(ValidateDeviceTokenRequestMsg.newBuilder().setToken(deviceToken).build(),
						new DeviceAuthCallback(transportContext, responseWriter, sessionInfo -> {
							TransportService transportService = transportContext.getTransportService();
							transportService.process(sessionInfo, JsonConverter.convertToTelemetryProto(start+2,getJsonObjectFromArray(arr.get(2).getAsJsonObject())),
									new HttpOkCallback(responseWriter));
						}));
				transportContext.getTransportService().process(ValidateDeviceTokenRequestMsg.newBuilder().setToken(deviceToken).build(),
						new DeviceAuthCallback(transportContext, responseWriter, sessionInfo -> {
							TransportService transportService = transportContext.getTransportService();
							transportService.process(sessionInfo, JsonConverter.convertToTelemetryProto(start+1,getJsonObjectFromArray(arr.get(1).getAsJsonObject())),
									new HttpOkCallback(responseWriter));
						}));
				transportContext.getTransportService().process(ValidateDeviceTokenRequestMsg.newBuilder().setToken(deviceToken).build(),
					new DeviceAuthCallback(transportContext, responseWriter, sessionInfo -> {
						TransportService transportService = transportContext.getTransportService();
						transportService.process(sessionInfo, JsonConverter.convertToTelemetryProto(start,getJsonObjectFromArray(arr.get(0).getAsJsonObject())),
								new HttpOkCallback(responseWriter));
					}));
				break;
			case 7:
				transportContext.getTransportService().process(ValidateDeviceTokenRequestMsg.newBuilder().setToken(deviceToken).build(),
						new DeviceAuthCallback(transportContext, responseWriter, sessionInfo -> {
							TransportService transportService = transportContext.getTransportService();
							transportService.process(sessionInfo, JsonConverter.convertToTelemetryProto(start+6,getJsonObjectFromArray(arr.get(6).getAsJsonObject())),
									new HttpOkCallback(responseWriter));
						}));
				transportContext.getTransportService().process(ValidateDeviceTokenRequestMsg.newBuilder().setToken(deviceToken).build(),
						new DeviceAuthCallback(transportContext, responseWriter, sessionInfo -> {
							TransportService transportService = transportContext.getTransportService();
							transportService.process(sessionInfo, JsonConverter.convertToTelemetryProto(start+5,getJsonObjectFromArray(arr.get(5).getAsJsonObject())),
									new HttpOkCallback(responseWriter));
						}));
				transportContext.getTransportService().process(ValidateDeviceTokenRequestMsg.newBuilder().setToken(deviceToken).build(),
						new DeviceAuthCallback(transportContext, responseWriter, sessionInfo -> {
							TransportService transportService = transportContext.getTransportService();
							transportService.process(sessionInfo, JsonConverter.convertToTelemetryProto(start+4,getJsonObjectFromArray(arr.get(4).getAsJsonObject())),
									new HttpOkCallback(responseWriter));
						}));
				transportContext.getTransportService().process(ValidateDeviceTokenRequestMsg.newBuilder().setToken(deviceToken).build(),
						new DeviceAuthCallback(transportContext, responseWriter, sessionInfo -> {
							TransportService transportService = transportContext.getTransportService();
							transportService.process(sessionInfo, JsonConverter.convertToTelemetryProto(start+3,getJsonObjectFromArray(arr.get(3).getAsJsonObject())),
									new HttpOkCallback(responseWriter));
						}));
				transportContext.getTransportService().process(ValidateDeviceTokenRequestMsg.newBuilder().setToken(deviceToken).build(),
						new DeviceAuthCallback(transportContext, responseWriter, sessionInfo -> {
							TransportService transportService = transportContext.getTransportService();
							transportService.process(sessionInfo, JsonConverter.convertToTelemetryProto(start+2,getJsonObjectFromArray(arr.get(2).getAsJsonObject())),
									new HttpOkCallback(responseWriter));
						}));
				transportContext.getTransportService().process(ValidateDeviceTokenRequestMsg.newBuilder().setToken(deviceToken).build(),
						new DeviceAuthCallback(transportContext, responseWriter, sessionInfo -> {
							TransportService transportService = transportContext.getTransportService();
							transportService.process(sessionInfo, JsonConverter.convertToTelemetryProto(start+1,getJsonObjectFromArray(arr.get(1).getAsJsonObject())),
									new HttpOkCallback(responseWriter));
						}));
				transportContext.getTransportService().process(ValidateDeviceTokenRequestMsg.newBuilder().setToken(deviceToken).build(),
					new DeviceAuthCallback(transportContext, responseWriter, sessionInfo -> {
						TransportService transportService = transportContext.getTransportService();
						transportService.process(sessionInfo, JsonConverter.convertToTelemetryProto(start,getJsonObjectFromArray(arr.get(0).getAsJsonObject())),
								new HttpOkCallback(responseWriter));
					}));
				break;
			case 8:
				transportContext.getTransportService().process(ValidateDeviceTokenRequestMsg.newBuilder().setToken(deviceToken).build(),
						new DeviceAuthCallback(transportContext, responseWriter, sessionInfo -> {
							TransportService transportService = transportContext.getTransportService();
							transportService.process(sessionInfo, JsonConverter.convertToTelemetryProto(start+7,getJsonObjectFromArray(arr.get(7).getAsJsonObject())),
									new HttpOkCallback(responseWriter));
						}));
				transportContext.getTransportService().process(ValidateDeviceTokenRequestMsg.newBuilder().setToken(deviceToken).build(),
						new DeviceAuthCallback(transportContext, responseWriter, sessionInfo -> {
							TransportService transportService = transportContext.getTransportService();
							transportService.process(sessionInfo, JsonConverter.convertToTelemetryProto(start+6,getJsonObjectFromArray(arr.get(6).getAsJsonObject())),
									new HttpOkCallback(responseWriter));
						}));
				transportContext.getTransportService().process(ValidateDeviceTokenRequestMsg.newBuilder().setToken(deviceToken).build(),
						new DeviceAuthCallback(transportContext, responseWriter, sessionInfo -> {
							TransportService transportService = transportContext.getTransportService();
							transportService.process(sessionInfo, JsonConverter.convertToTelemetryProto(start+5,getJsonObjectFromArray(arr.get(5).getAsJsonObject())),
									new HttpOkCallback(responseWriter));
						}));
				transportContext.getTransportService().process(ValidateDeviceTokenRequestMsg.newBuilder().setToken(deviceToken).build(),
						new DeviceAuthCallback(transportContext, responseWriter, sessionInfo -> {
							TransportService transportService = transportContext.getTransportService();
							transportService.process(sessionInfo, JsonConverter.convertToTelemetryProto(start+4,getJsonObjectFromArray(arr.get(4).getAsJsonObject())),
									new HttpOkCallback(responseWriter));
						}));
				transportContext.getTransportService().process(ValidateDeviceTokenRequestMsg.newBuilder().setToken(deviceToken).build(),
						new DeviceAuthCallback(transportContext, responseWriter, sessionInfo -> {
							TransportService transportService = transportContext.getTransportService();
							transportService.process(sessionInfo, JsonConverter.convertToTelemetryProto(start+3,getJsonObjectFromArray(arr.get(3).getAsJsonObject())),
									new HttpOkCallback(responseWriter));
						}));
				transportContext.getTransportService().process(ValidateDeviceTokenRequestMsg.newBuilder().setToken(deviceToken).build(),
						new DeviceAuthCallback(transportContext, responseWriter, sessionInfo -> {
							TransportService transportService = transportContext.getTransportService();
							transportService.process(sessionInfo, JsonConverter.convertToTelemetryProto(start+2,getJsonObjectFromArray(arr.get(2).getAsJsonObject())),
									new HttpOkCallback(responseWriter));
						}));
				transportContext.getTransportService().process(ValidateDeviceTokenRequestMsg.newBuilder().setToken(deviceToken).build(),
						new DeviceAuthCallback(transportContext, responseWriter, sessionInfo -> {
							TransportService transportService = transportContext.getTransportService();
							transportService.process(sessionInfo, JsonConverter.convertToTelemetryProto(start+1,getJsonObjectFromArray(arr.get(1).getAsJsonObject())),
									new HttpOkCallback(responseWriter));
						}));
				transportContext.getTransportService().process(ValidateDeviceTokenRequestMsg.newBuilder().setToken(deviceToken).build(),
					new DeviceAuthCallback(transportContext, responseWriter, sessionInfo -> {
						TransportService transportService = transportContext.getTransportService();
						transportService.process(sessionInfo, JsonConverter.convertToTelemetryProto(start,getJsonObjectFromArray(arr.get(0).getAsJsonObject())),
								new HttpOkCallback(responseWriter));
					}));
				break;
			case 9:
				transportContext.getTransportService().process(ValidateDeviceTokenRequestMsg.newBuilder().setToken(deviceToken).build(),
						new DeviceAuthCallback(transportContext, responseWriter, sessionInfo -> {
							TransportService transportService = transportContext.getTransportService();
							transportService.process(sessionInfo, JsonConverter.convertToTelemetryProto(start+8,getJsonObjectFromArray(arr.get(8).getAsJsonObject())),
									new HttpOkCallback(responseWriter));
						}));
				transportContext.getTransportService().process(ValidateDeviceTokenRequestMsg.newBuilder().setToken(deviceToken).build(),
						new DeviceAuthCallback(transportContext, responseWriter, sessionInfo -> {
							TransportService transportService = transportContext.getTransportService();
							transportService.process(sessionInfo, JsonConverter.convertToTelemetryProto(start+7,getJsonObjectFromArray(arr.get(7).getAsJsonObject())),
									new HttpOkCallback(responseWriter));
						}));
				transportContext.getTransportService().process(ValidateDeviceTokenRequestMsg.newBuilder().setToken(deviceToken).build(),
						new DeviceAuthCallback(transportContext, responseWriter, sessionInfo -> {
							TransportService transportService = transportContext.getTransportService();
							transportService.process(sessionInfo, JsonConverter.convertToTelemetryProto(start+6,getJsonObjectFromArray(arr.get(6).getAsJsonObject())),
									new HttpOkCallback(responseWriter));
						}));
				transportContext.getTransportService().process(ValidateDeviceTokenRequestMsg.newBuilder().setToken(deviceToken).build(),
						new DeviceAuthCallback(transportContext, responseWriter, sessionInfo -> {
							TransportService transportService = transportContext.getTransportService();
							transportService.process(sessionInfo, JsonConverter.convertToTelemetryProto(start+5,getJsonObjectFromArray(arr.get(5).getAsJsonObject())),
									new HttpOkCallback(responseWriter));
						}));
				transportContext.getTransportService().process(ValidateDeviceTokenRequestMsg.newBuilder().setToken(deviceToken).build(),
						new DeviceAuthCallback(transportContext, responseWriter, sessionInfo -> {
							TransportService transportService = transportContext.getTransportService();
							transportService.process(sessionInfo, JsonConverter.convertToTelemetryProto(start+4,getJsonObjectFromArray(arr.get(4).getAsJsonObject())),
									new HttpOkCallback(responseWriter));
						}));
				transportContext.getTransportService().process(ValidateDeviceTokenRequestMsg.newBuilder().setToken(deviceToken).build(),
						new DeviceAuthCallback(transportContext, responseWriter, sessionInfo -> {
							TransportService transportService = transportContext.getTransportService();
							transportService.process(sessionInfo, JsonConverter.convertToTelemetryProto(start+3,getJsonObjectFromArray(arr.get(3).getAsJsonObject())),
									new HttpOkCallback(responseWriter));
						}));
				transportContext.getTransportService().process(ValidateDeviceTokenRequestMsg.newBuilder().setToken(deviceToken).build(),
						new DeviceAuthCallback(transportContext, responseWriter, sessionInfo -> {
							TransportService transportService = transportContext.getTransportService();
							transportService.process(sessionInfo, JsonConverter.convertToTelemetryProto(start+2,getJsonObjectFromArray(arr.get(2).getAsJsonObject())),
									new HttpOkCallback(responseWriter));
						}));
				transportContext.getTransportService().process(ValidateDeviceTokenRequestMsg.newBuilder().setToken(deviceToken).build(),
						new DeviceAuthCallback(transportContext, responseWriter, sessionInfo -> {
							TransportService transportService = transportContext.getTransportService();
							transportService.process(sessionInfo, JsonConverter.convertToTelemetryProto(start+1,getJsonObjectFromArray(arr.get(1).getAsJsonObject())),
									new HttpOkCallback(responseWriter));
						}));
				transportContext.getTransportService().process(ValidateDeviceTokenRequestMsg.newBuilder().setToken(deviceToken).build(),
					new DeviceAuthCallback(transportContext, responseWriter, sessionInfo -> {
						TransportService transportService = transportContext.getTransportService();
						transportService.process(sessionInfo, JsonConverter.convertToTelemetryProto(start,getJsonObjectFromArray(arr.get(0).getAsJsonObject())),
								new HttpOkCallback(responseWriter));
					}));
				break;
			case 10:
				transportContext.getTransportService().process(ValidateDeviceTokenRequestMsg.newBuilder().setToken(deviceToken).build(),
						new DeviceAuthCallback(transportContext, responseWriter, sessionInfo -> {
							TransportService transportService = transportContext.getTransportService();
							transportService.process(sessionInfo, JsonConverter.convertToTelemetryProto(start+9,getJsonObjectFromArray(arr.get(9).getAsJsonObject())),
									new HttpOkCallback(responseWriter));
						}));
				transportContext.getTransportService().process(ValidateDeviceTokenRequestMsg.newBuilder().setToken(deviceToken).build(),
						new DeviceAuthCallback(transportContext, responseWriter, sessionInfo -> {
							TransportService transportService = transportContext.getTransportService();
							transportService.process(sessionInfo, JsonConverter.convertToTelemetryProto(start+8,getJsonObjectFromArray(arr.get(8).getAsJsonObject())),
									new HttpOkCallback(responseWriter));
						}));
				transportContext.getTransportService().process(ValidateDeviceTokenRequestMsg.newBuilder().setToken(deviceToken).build(),
						new DeviceAuthCallback(transportContext, responseWriter, sessionInfo -> {
							TransportService transportService = transportContext.getTransportService();
							transportService.process(sessionInfo, JsonConverter.convertToTelemetryProto(start+7,getJsonObjectFromArray(arr.get(7).getAsJsonObject())),
									new HttpOkCallback(responseWriter));
						}));
				transportContext.getTransportService().process(ValidateDeviceTokenRequestMsg.newBuilder().setToken(deviceToken).build(),
						new DeviceAuthCallback(transportContext, responseWriter, sessionInfo -> {
							TransportService transportService = transportContext.getTransportService();
							transportService.process(sessionInfo, JsonConverter.convertToTelemetryProto(start+6,getJsonObjectFromArray(arr.get(6).getAsJsonObject())),
									new HttpOkCallback(responseWriter));
						}));
				transportContext.getTransportService().process(ValidateDeviceTokenRequestMsg.newBuilder().setToken(deviceToken).build(),
						new DeviceAuthCallback(transportContext, responseWriter, sessionInfo -> {
							TransportService transportService = transportContext.getTransportService();
							transportService.process(sessionInfo, JsonConverter.convertToTelemetryProto(start+5,getJsonObjectFromArray(arr.get(5).getAsJsonObject())),
									new HttpOkCallback(responseWriter));
						}));
				transportContext.getTransportService().process(ValidateDeviceTokenRequestMsg.newBuilder().setToken(deviceToken).build(),
						new DeviceAuthCallback(transportContext, responseWriter, sessionInfo -> {
							TransportService transportService = transportContext.getTransportService();
							transportService.process(sessionInfo, JsonConverter.convertToTelemetryProto(start+4,getJsonObjectFromArray(arr.get(4).getAsJsonObject())),
									new HttpOkCallback(responseWriter));
						}));
				transportContext.getTransportService().process(ValidateDeviceTokenRequestMsg.newBuilder().setToken(deviceToken).build(),
						new DeviceAuthCallback(transportContext, responseWriter, sessionInfo -> {
							TransportService transportService = transportContext.getTransportService();
							transportService.process(sessionInfo, JsonConverter.convertToTelemetryProto(start+3,getJsonObjectFromArray(arr.get(3).getAsJsonObject())),
									new HttpOkCallback(responseWriter));
						}));
				transportContext.getTransportService().process(ValidateDeviceTokenRequestMsg.newBuilder().setToken(deviceToken).build(),
						new DeviceAuthCallback(transportContext, responseWriter, sessionInfo -> {
							TransportService transportService = transportContext.getTransportService();
							transportService.process(sessionInfo, JsonConverter.convertToTelemetryProto(start+2,getJsonObjectFromArray(arr.get(2).getAsJsonObject())),
									new HttpOkCallback(responseWriter));
						}));
				transportContext.getTransportService().process(ValidateDeviceTokenRequestMsg.newBuilder().setToken(deviceToken).build(),
						new DeviceAuthCallback(transportContext, responseWriter, sessionInfo -> {
							TransportService transportService = transportContext.getTransportService();
							transportService.process(sessionInfo, JsonConverter.convertToTelemetryProto(start+1,getJsonObjectFromArray(arr.get(1).getAsJsonObject())),
									new HttpOkCallback(responseWriter));
						}));
				transportContext.getTransportService().process(ValidateDeviceTokenRequestMsg.newBuilder().setToken(deviceToken).build(),
					new DeviceAuthCallback(transportContext, responseWriter, sessionInfo -> {
						TransportService transportService = transportContext.getTransportService();
						transportService.process(sessionInfo, JsonConverter.convertToTelemetryProto(start,getJsonObjectFromArray(arr.get(0).getAsJsonObject())),
								new HttpOkCallback(responseWriter));
					}));
				break;
		}
        return responseWriter;
    }

    @RequestMapping(value = "/{deviceToken}/claim", method = RequestMethod.POST)
    public DeferredResult<ResponseEntity> claimDevice(@PathVariable("deviceToken") String deviceToken,
                                                      @RequestBody(required = false) String json, HttpServletRequest request) {
        DeferredResult<ResponseEntity> responseWriter = new DeferredResult<>();
        transportContext.getTransportService().process(ValidateDeviceTokenRequestMsg.newBuilder().setToken(deviceToken).build(),
                new DeviceAuthCallback(transportContext, responseWriter, sessionInfo -> {
                    TransportService transportService = transportContext.getTransportService();
                    DeviceId deviceId = new DeviceId(new UUID(sessionInfo.getDeviceIdMSB(), sessionInfo.getDeviceIdLSB()));
                    transportService.process(sessionInfo, JsonConverter.convertToClaimDeviceProto(deviceId, json),
                            new HttpOkCallback(responseWriter));
                }));
        return responseWriter;
    }

    @RequestMapping(value = "/{deviceToken}/rpc", method = RequestMethod.GET, produces = "application/json")
    public DeferredResult<ResponseEntity> subscribeToCommands(@PathVariable("deviceToken") String deviceToken,
                                                              @RequestParam(value = "timeout", required = false, defaultValue = "0") long timeout,
                                                              HttpServletRequest httpRequest) {
        DeferredResult<ResponseEntity> responseWriter = new DeferredResult<>();
        transportContext.getTransportService().process(ValidateDeviceTokenRequestMsg.newBuilder().setToken(deviceToken).build(),
                new DeviceAuthCallback(transportContext, responseWriter, sessionInfo -> {
                    TransportService transportService = transportContext.getTransportService();
                    transportService.registerSyncSession(sessionInfo, new HttpSessionListener(responseWriter),
                            timeout == 0 ? transportContext.getDefaultTimeout() : timeout);
                    transportService.process(sessionInfo, SubscribeToRPCMsg.getDefaultInstance(),
                            new SessionCloseOnErrorCallback(transportService, sessionInfo));

                }));
        return responseWriter;
    }

    @RequestMapping(value = "/{deviceToken}/rpc/{requestId}", method = RequestMethod.POST)
    public DeferredResult<ResponseEntity> replyToCommand(@PathVariable("deviceToken") String deviceToken,
                                                         @PathVariable("requestId") Integer requestId,
                                                         @RequestBody String json, HttpServletRequest request) {
        DeferredResult<ResponseEntity> responseWriter = new DeferredResult<ResponseEntity>();
        transportContext.getTransportService().process(ValidateDeviceTokenRequestMsg.newBuilder().setToken(deviceToken).build(),
                new DeviceAuthCallback(transportContext, responseWriter, sessionInfo -> {
                    TransportService transportService = transportContext.getTransportService();
                    transportService.process(sessionInfo, ToDeviceRpcResponseMsg.newBuilder().setRequestId(requestId).setPayload(json).build(), new HttpOkCallback(responseWriter));
                }));
        return responseWriter;
    }

    @RequestMapping(value = "/{deviceToken}/rpc", method = RequestMethod.POST)
    public DeferredResult<ResponseEntity> postRpcRequest(@PathVariable("deviceToken") String deviceToken,
                                                         @RequestBody String json, HttpServletRequest httpRequest) {
        DeferredResult<ResponseEntity> responseWriter = new DeferredResult<ResponseEntity>();
        transportContext.getTransportService().process(ValidateDeviceTokenRequestMsg.newBuilder().setToken(deviceToken).build(),
                new DeviceAuthCallback(transportContext, responseWriter, sessionInfo -> {
                    JsonObject request = new JsonParser().parse(json).getAsJsonObject();
                    TransportService transportService = transportContext.getTransportService();
                    transportService.registerSyncSession(sessionInfo, new HttpSessionListener(responseWriter), transportContext.getDefaultTimeout());
                    transportService.process(sessionInfo, ToServerRpcRequestMsg.newBuilder().setRequestId(0)
                                    .setMethodName(request.get("method").getAsString())
                                    .setParams(request.get("params").toString()).build(),
                            new SessionCloseOnErrorCallback(transportService, sessionInfo));
                }));
        return responseWriter;
    }

    @RequestMapping(value = "/{deviceToken}/attributes/updates", method = RequestMethod.GET, produces = "application/json")
    public DeferredResult<ResponseEntity> subscribeToAttributes(@PathVariable("deviceToken") String deviceToken,
                                                                @RequestParam(value = "timeout", required = false, defaultValue = "0") long timeout,
                                                                HttpServletRequest httpRequest) {
        DeferredResult<ResponseEntity> responseWriter = new DeferredResult<>();
        transportContext.getTransportService().process(ValidateDeviceTokenRequestMsg.newBuilder().setToken(deviceToken).build(),
                new DeviceAuthCallback(transportContext, responseWriter, sessionInfo -> {
                    TransportService transportService = transportContext.getTransportService();
                    transportService.registerSyncSession(sessionInfo, new HttpSessionListener(responseWriter),
                            timeout == 0 ? transportContext.getDefaultTimeout() : timeout);
                    transportService.process(sessionInfo, SubscribeToAttributeUpdatesMsg.getDefaultInstance(),
                            new SessionCloseOnErrorCallback(transportService, sessionInfo));

                }));
        return responseWriter;
    }

    private static class DeviceAuthCallback implements TransportServiceCallback<ValidateDeviceCredentialsResponseMsg> {
        private final TransportContext transportContext;
        private final DeferredResult<ResponseEntity> responseWriter;
        private final Consumer<SessionInfoProto> onSuccess;

        DeviceAuthCallback(TransportContext transportContext, DeferredResult<ResponseEntity> responseWriter, Consumer<SessionInfoProto> onSuccess) {
            this.transportContext = transportContext;
            this.responseWriter = responseWriter;
            this.onSuccess = onSuccess;
        }

        @Override
        public void onSuccess(ValidateDeviceCredentialsResponseMsg msg) {
            if (msg.hasDeviceInfo()) {
                UUID sessionId = UUID.randomUUID();
                DeviceInfoProto deviceInfoProto = msg.getDeviceInfo();
                SessionInfoProto sessionInfo = SessionInfoProto.newBuilder()
                        .setNodeId(transportContext.getNodeId())
                        .setTenantIdMSB(deviceInfoProto.getTenantIdMSB())
                        .setTenantIdLSB(deviceInfoProto.getTenantIdLSB())
                        .setDeviceIdMSB(deviceInfoProto.getDeviceIdMSB())
                        .setDeviceIdLSB(deviceInfoProto.getDeviceIdLSB())
                        .setSessionIdMSB(sessionId.getMostSignificantBits())
                        .setSessionIdLSB(sessionId.getLeastSignificantBits())
                        .build();
                onSuccess.accept(sessionInfo);
            } else {
                responseWriter.setResult(new ResponseEntity<>(HttpStatus.UNAUTHORIZED));
            }
        }

        @Override
        public void onError(Throwable e) {
            log.warn("Failed to process request", e);
            responseWriter.setResult(new ResponseEntity<>(HttpStatus.INTERNAL_SERVER_ERROR));
        }
    }

    private static class SessionCloseOnErrorCallback implements TransportServiceCallback<Void> {
        private final TransportService transportService;
        private final SessionInfoProto sessionInfo;

        SessionCloseOnErrorCallback(TransportService transportService, SessionInfoProto sessionInfo) {
            this.transportService = transportService;
            this.sessionInfo = sessionInfo;
        }

        @Override
        public void onSuccess(Void msg) {
        }

        @Override
        public void onError(Throwable e) {
            transportService.deregisterSession(sessionInfo);
        }
    }

    private static class HttpOkCallback implements TransportServiceCallback<Void> {
        private final DeferredResult<ResponseEntity> responseWriter;

        public HttpOkCallback(DeferredResult<ResponseEntity> responseWriter) {
            this.responseWriter = responseWriter;
        }

        @Override
        public void onSuccess(Void msg) {
            responseWriter.setResult(new ResponseEntity<>(HttpStatus.OK));
        }

        @Override
        public void onError(Throwable e) {
            responseWriter.setResult(new ResponseEntity<>(HttpStatus.INTERNAL_SERVER_ERROR));
        }
    }


    private static class HttpSessionListener implements SessionMsgListener {

        private final DeferredResult<ResponseEntity> responseWriter;

        HttpSessionListener(DeferredResult<ResponseEntity> responseWriter) {
            this.responseWriter = responseWriter;
        }

        @Override
        public void onGetAttributesResponse(GetAttributeResponseMsg msg) {
            responseWriter.setResult(new ResponseEntity<>(JsonConverter.toJson(msg).toString(), HttpStatus.OK));
        }

        @Override
        public void onAttributeUpdate(AttributeUpdateNotificationMsg msg) {
            responseWriter.setResult(new ResponseEntity<>(JsonConverter.toJson(msg).toString(), HttpStatus.OK));
        }

        @Override
        public void onRemoteSessionCloseCommand(SessionCloseNotificationProto sessionCloseNotification) {
            responseWriter.setResult(new ResponseEntity<>(HttpStatus.REQUEST_TIMEOUT));
        }

        @Override
        public void onToDeviceRpcRequest(ToDeviceRpcRequestMsg msg) {
            responseWriter.setResult(new ResponseEntity<>(JsonConverter.toJson(msg, true).toString(), HttpStatus.OK));
        }

        @Override
        public void onToServerRpcResponse(ToServerRpcResponseMsg msg) {
            responseWriter.setResult(new ResponseEntity<>(JsonConverter.toJson(msg).toString(), HttpStatus.OK));
        }
    }
}
