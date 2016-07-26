package com.packt.firstyarnapp;

import java.io.File;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.YarnClientApplication;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.Apps;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;

public class Client {

	public static void main(String[] args) throws Exception {
		try {
			Client clientObj = new Client();
			if (clientObj.run(args)) {
				System.out.println("Application completed successfully");
			} else {
				System.out.println("Application Failed / Killed");
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public boolean run(String[] args) throws Exception {
		// Point #1
		final String command = args[0];
		final int n = Integer.valueOf(args[1]);
		final Path jarPath = new Path(args[2]);

		System.out.println("Initializing YARN configuration");
		YarnConfiguration conf = new YarnConfiguration();
		YarnClient yarnClient = YarnClient.createYarnClient();
		yarnClient.init(conf);
		yarnClient.start();

		// Point #2
		System.out.println("Requesting ResourceManager for a new Application");
		YarnClientApplication app = yarnClient.createApplication();

		// Point #3
		System.out.println("Initializing ContainerLaunchContext for ApplicationMaster container");
		ContainerLaunchContext amContainer = Records
				.newRecord(ContainerLaunchContext.class);

		System.out.println("Adding LocalResource");
		LocalResource appMasterJar = Records.newRecord(LocalResource.class);
		FileStatus jarStat = FileSystem.get(conf).getFileStatus(jarPath);
		appMasterJar.setResource(ConverterUtils.getYarnUrlFromPath(jarPath));
		appMasterJar.setSize(jarStat.getLen());
		appMasterJar.setTimestamp(jarStat.getModificationTime());
		appMasterJar.setType(LocalResourceType.FILE);
		appMasterJar.setVisibility(LocalResourceVisibility.PUBLIC);

		// Point #4
		System.out.println("Setting environment");
		Map<String, String> appMasterEnv = new HashMap<String, String>();
		for (String c : conf.getStrings(
				YarnConfiguration.YARN_APPLICATION_CLASSPATH,
				YarnConfiguration.DEFAULT_YARN_APPLICATION_CLASSPATH)) {
			Apps.addToEnvironment(appMasterEnv, Environment.CLASSPATH.name(),
					c.trim());
		}

		Apps.addToEnvironment(appMasterEnv, Environment.CLASSPATH.name(),
				Environment.PWD.$() + File.separator + "*");

		System.out.println("Setting resource capability");
		Resource capability = Records.newRecord(Resource.class);
		capability.setMemory(256);
		capability.setVirtualCores(1);

		System.out.println("Setting command to start ApplicationMaster service");
		amContainer.setCommands(Collections.singletonList("/usr/lib/jvm/jdk1.8.0/bin/java"
				+ " -Xmx256M" + " com.packt.firstyarnapp.ApplicationMaster"
				+ " " + command + " " + String.valueOf(n) + " 1>"
				+ ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stdout"
				+ " 2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR
				+ "/stderr"));
		amContainer.setLocalResources(Collections.singletonMap(
				"first-yarn-app.jar", appMasterJar));
		amContainer.setEnvironment(appMasterEnv);

		System.out.println("Initializing ApplicationSubmissionContext");
		ApplicationSubmissionContext appContext = app
				.getApplicationSubmissionContext();
		appContext.setApplicationName("first-yarn-app");
		appContext.setApplicationType("YARN");
		appContext.setAMContainerSpec(amContainer);
		appContext.setResource(capability);
		appContext.setQueue("default");

		ApplicationId appId = appContext.getApplicationId();
		System.out.println("Submitting application " + appId);
		yarnClient.submitApplication(appContext);

		ApplicationReport appReport = yarnClient.getApplicationReport(appId);
		YarnApplicationState appState = appReport.getYarnApplicationState();
		while (appState != YarnApplicationState.FINISHED
				&& appState != YarnApplicationState.KILLED
				&& appState != YarnApplicationState.FAILED) {
			Thread.sleep(100);
			appReport = yarnClient.getApplicationReport(appId);
			appState = appReport.getYarnApplicationState();
		}
		if (appState == YarnApplicationState.FINISHED) {
			return true;
		} else {
			return false;
		}
	}
}
