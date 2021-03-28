import com.refinitiv.ema.access.AckMsg;
import com.refinitiv.ema.access.ElementList;
import com.refinitiv.ema.access.EmaFactory;
import com.refinitiv.ema.access.GenericMsg;
import com.refinitiv.ema.access.Map;
import com.refinitiv.ema.access.MapEntry;
import com.refinitiv.ema.access.Msg;
import com.refinitiv.ema.access.OmmConsumer;
import com.refinitiv.ema.access.OmmConsumerClient;
import com.refinitiv.ema.access.OmmConsumerConfig;
import com.refinitiv.ema.access.OmmConsumerEvent;
import com.refinitiv.ema.access.RefreshMsg;
import com.refinitiv.ema.access.ReqMsg;
import com.refinitiv.ema.access.ServiceEndpointDiscovery;
import com.refinitiv.ema.access.ServiceEndpointDiscoveryClient;
import com.refinitiv.ema.access.ServiceEndpointDiscoveryEvent;
import com.refinitiv.ema.access.ServiceEndpointDiscoveryInfo;
import com.refinitiv.ema.access.ServiceEndpointDiscoveryOption;
import com.refinitiv.ema.access.ServiceEndpointDiscoveryResp;
import com.refinitiv.ema.access.StatusMsg;
import com.refinitiv.ema.access.UpdateMsg;

class AppClient implements ServiceEndpointDiscoveryClient, OmmConsumerClient{

	String host;
	String port;
	@Override
	public void onError(String errText, ServiceEndpointDiscoveryEvent event) {
		System.out.println("Failed to get endpoints:" + errText);
	}

	@Override
	public void onSuccess(ServiceEndpointDiscoveryResp serviceEndpointResp, ServiceEndpointDiscoveryEvent event) {
		//System.out.println(serviceEndpointResp);
		for (ServiceEndpointDiscoveryInfo info:serviceEndpointResp.serviceEndpointInfoList()) {
			if (info.transport().equals("tcp")) {
				//print out only host and port for TCP transport type
//				System.out.println(info.endpoint() + ":" + info.port());
				host = info.endpoint();
				port = info.port();
				break;
			}
			else {
				continue;
			}
			
		}
		
	}

	@Override
	public void onAckMsg(AckMsg arg0, OmmConsumerEvent arg1) {
	}

	@Override
	public void onAllMsg(Msg arg0, OmmConsumerEvent arg1) {
	}

	@Override
	public void onGenericMsg(GenericMsg arg0, OmmConsumerEvent arg1) {
	}

	@Override
	public void onRefreshMsg(RefreshMsg msg, OmmConsumerEvent arg1) {
		System.out.println(msg);
	}

	@Override
	public void onStatusMsg(StatusMsg msg, OmmConsumerEvent arg1) {
		System.out.println(msg);
	}

	@Override
	public void onUpdateMsg(UpdateMsg msg, OmmConsumerEvent arg1) {
		System.out.println(msg);
	}
	
}
public class ConsumerRTO {

	public static void main(String[] args) {
		//Get Service Endpoint
		//machine id, password, client id
		String username = "GE-A-001XXXX-X-XXXX";
		String password = "ThisisADemoPasswordNotAValidPassword";
		String clientId = "123456abcdef";
		
		//prepare appclient
		AppClient appClient = new AppClient();
		
		//set options
		ServiceEndpointDiscoveryOption options =  EmaFactory.createServiceEndpointDiscoveryOption();
		options.username(username);
		options.password(password);
		options.clientId(clientId);
		
		//use ServiceEndpoint from EMA
		ServiceEndpointDiscovery service = EmaFactory.createServiceEndpointDiscovery();
		service.registerClient(options, appClient);
		
		System.out.println("Host:" + appClient.host);
		System.out.println("Port:" + appClient.port);
		
		//Configuration Preparation for OmmConsumer using Host, Port.
		Map configDb = EmaFactory.createMap();
		Map elementMap = EmaFactory.createMap();
		ElementList elementList = EmaFactory.createElementList();
		ElementList innerElementList = EmaFactory.createElementList();
		innerElementList.add(EmaFactory.createElementEntry().ascii("Channel", "Channel_1"));
		elementMap.add(EmaFactory.createMapEntry().keyAscii("Consumer_1", MapEntry.MapAction.ADD, innerElementList));
		innerElementList.clear();
		elementList.add(EmaFactory.createElementEntry().map("ConsumerList", elementMap));
		elementMap.clear();
		configDb.add(EmaFactory.createMapEntry().keyAscii("ConsumerGroup", MapEntry.MapAction.ADD, elementList));
		elementList.clear();
		innerElementList.add(EmaFactory.createElementEntry().ascii("ChannelType", "ChannelType::RSSL_ENCRYPTED"));
		innerElementList.add(EmaFactory.createElementEntry().ascii("Host", appClient.host));
		innerElementList.add(EmaFactory.createElementEntry().ascii("Port", appClient.port));
		innerElementList.add(EmaFactory.createElementEntry().intValue("EnableSessionManagement", 1));
		elementMap.add(EmaFactory.createMapEntry().keyAscii("Channel_1", MapEntry.MapAction.ADD, innerElementList));
		innerElementList.clear();
		elementList.add(EmaFactory.createElementEntry().map("ChannelList", elementMap));
		elementMap.clear();
		configDb.add(EmaFactory.createMapEntry().keyAscii("ChannelGroup", MapEntry.MapAction.ADD, elementList));
		elementList.clear();
		
		OmmConsumerConfig config = EmaFactory.createOmmConsumerConfig();
		config.consumerName("Consumer_1");
		config.username(username);
		config.password(password);
		config.clientId(clientId);
		config.config(configDb);
		
		//Connect to Real-Time Optimized using config
		OmmConsumer consumer = EmaFactory.createOmmConsumer(config);
		
		//Data subscription
		ReqMsg req1 = EmaFactory.createReqMsg();
		req1.name("JPY=");
		req1.serviceName("ELEKTRON_DD");
		consumer.registerClient(req1, appClient);
		
		try {
			Thread.sleep(6000000);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
