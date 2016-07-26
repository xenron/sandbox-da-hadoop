package hadoop.yarn.rest.client;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;

import org.json.simple.JSONObject;

public class SampleClient {

	public static void main(String[] args) {

		try {
			long waitTime = 10000;
			CallableRequest callableRequest = new CallableRequest(args[0]);
			FutureTask<JSONObject> getRequestTask = new FutureTask<JSONObject>(
					callableRequest);

			ExecutorService executor = Executors.newFixedThreadPool(1);
			executor.execute(getRequestTask);
			
			JSONObject beanObject = getRequestTask.get(waitTime,
					TimeUnit.MILLISECONDS);
			if (beanObject == null) {
				System.out.println("Error: Unable to get JSON response.");
			} else {
				System.out.println("JSON Response:");
				System.out.println(beanObject.toJSONString());
			}	
		} catch (Exception e) {
			System.out.println("Exception: " + e.getMessage());
			e.printStackTrace();
		}
		System.exit(0);
	}
}