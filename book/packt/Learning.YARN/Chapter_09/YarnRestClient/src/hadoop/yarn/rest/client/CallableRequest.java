package hadoop.yarn.rest.client;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.concurrent.Callable;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

public class CallableRequest implements Callable<JSONObject> {

	String url;

	public CallableRequest(String url) {
		this.url = url;
	}

	private String getRequest(String urlPath, String input) {
		HttpURLConnection conn = null;
		String output = "";
		OutputStream os = null;
		try {
			URL url = new URL(urlPath);
			conn = (HttpURLConnection) url.openConnection();
			conn.setDoOutput(true);
			conn.setRequestMethod("GET");
			conn.setRequestProperty("Accept", "application/json");
			conn.setRequestProperty("Content-type", "application/json");
			if (input != null && !input.isEmpty()) {
				os = conn.getOutputStream();
				os.write(input.getBytes());
				os.flush();
			}
			String buffer = "";
			BufferedReader br;

			br = new BufferedReader(new InputStreamReader(
					(conn.getInputStream())));

			while ((buffer = br.readLine()) != null) {
				output += buffer;
			}
		} catch (Exception e) {
			return null;
		} finally {
			if (conn != null) {
				conn.disconnect();
			}
			if (os != null) {
				try {
					os.close();
				} catch (Exception e) {
					// TODO: handle exception
				}
			}
		}
		return output;
	}

	@Override
	public JSONObject call() throws Exception {
		JSONObject json = null;

		String data = this.getRequest(url, null);

		if (data == null) {
			throw new Exception("Could not fetch data from " + url);
		} else {
			json = (JSONObject) new JSONParser().parse(data);
		}
		return json;
	}
}