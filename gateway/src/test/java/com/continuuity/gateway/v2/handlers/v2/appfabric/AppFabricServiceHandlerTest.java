package com.continuuity.gateway.v2.handlers.v2.appfabric;

import com.continuuity.gateway.GatewayFastTestsSuite;
import com.continuuity.gateway.auth.GatewayAuthenticator;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.io.ByteStreams;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import junit.framework.Assert;
import org.apache.commons.io.FileUtils;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.util.EntityUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.util.List;
import java.util.Map;

/**
 * Testing of App Fabric REST Endpoints.
 */
public class AppFabricServiceHandlerTest {

  /**
   * Deploys and application.
   */
  private HttpResponse deploy(String filename) throws Exception {
    File archive = FileUtils.toFile(getClass().getResource("/" + filename));
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    try {
      ByteStreams.copy(new FileInputStream(archive), bos);
    } finally {
      bos.close();
    }

    HttpPut put = GatewayFastTestsSuite.getPUT("/v2/apps");
    put.setHeader(GatewayAuthenticator.CONTINUUITY_API_KEY, "api-key-example");
    put.setHeader("X-Archive-Name", filename);
    put.setEntity(new ByteArrayEntity(bos.toByteArray()));
    return GatewayFastTestsSuite.PUT(put);
  }

  /**
   * Tests deploying an application.
   */
  @Test
  public void testDeploy() throws Exception {
    HttpResponse response = deploy("WordCount.jar");
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
  }

  /**
   * Tests deleting an application.
   */
  @Test
  public void testDeleteApp() throws Exception {
    HttpResponse response = deploy("WordCount.jar");
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    Assert.assertEquals(200, GatewayFastTestsSuite.DELETE("/v2/apps/WordCountApp").getStatusLine().getStatusCode());
  }

  /**
   * Test deleting of all applications.
   */
  @Test
  public void testDeleteAllApps() throws Exception {
    HttpResponse response = deploy("WordCount.jar");
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    Assert.assertEquals(200, GatewayFastTestsSuite.DELETE("/v2/apps").getStatusLine().getStatusCode());
  }

  /**
   * @return Runnable status
   */
  private String getRunnableStatus(String runnableType, String appId, String runnableId) throws Exception {
    HttpResponse response =
      GatewayFastTestsSuite.GET("/v2/apps/" + appId + "/" + runnableType + "/" + runnableId + "/status");
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    String s = EntityUtils.toString(response.getEntity());
    Map<String, String> o = new Gson().fromJson(s, new TypeToken<Map<String, String>>() {}.getType());
    return o.get("status");
  }

  /**
   * Tests deploying a flow, starting a flow, stopping a flow and deleting the application.
   */
  @Test
  public void testStartStopStatusOfFlow() throws Exception {
    HttpResponse response = deploy("WordCount.jar");
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    try {
      Assert.assertEquals(200,
             GatewayFastTestsSuite.POST("/v2/apps/WordCountApp/flows/WordCountFlow/start", null)
               .getStatusLine().getStatusCode()
      );
      Assert.assertEquals("RUNNING", getRunnableStatus("flows", "WordCountApp", "WordCountFlow"));
    } finally {
      Assert.assertEquals(200,
                          GatewayFastTestsSuite.POST("/v2/apps/WordCountApp/flows/WordCountFlow/stop", null)
                            .getStatusLine().getStatusCode()
      );
      Assert.assertEquals("STOPPED", getRunnableStatus("flows", "WordCountApp", "WordCountFlow"));
      Assert.assertEquals(200, GatewayFastTestsSuite.DELETE("/v2/apps/WordCountApp").getStatusLine().getStatusCode());
      Assert.assertEquals(404, GatewayFastTestsSuite.DELETE("/v2/apps/WordCountApp").getStatusLine().getStatusCode());
    }
  }

  @Test
  public void testRunnableHistory() throws Exception {
    HttpResponse response = deploy("WordCount.jar");
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
  }

  @Test
  public void testGetFlowletInstances() throws Exception {

  }

  @Test
  public void testSetFlowletInstances() throws Exception {

  }

  @Test
  public void testRunnableStatus() throws Exception {

  }

  @Test
  public void testRunnableSpecification() throws Exception {

  }

  @Test
  public void testRunnableStart() throws Exception {

  }

  @Test
  public void testRunnableStop() throws Exception {

  }
}
