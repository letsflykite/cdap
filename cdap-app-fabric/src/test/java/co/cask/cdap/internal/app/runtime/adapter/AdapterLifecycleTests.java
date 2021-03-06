/*
 * Copyright © 2015 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package co.cask.cdap.internal.app.runtime.adapter;

import co.cask.cdap.AppWithServices;
import co.cask.cdap.DummyTemplate;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.io.Locations;
import co.cask.cdap.internal.app.services.http.AppFabricTestBase;
import co.cask.cdap.internal.test.AppJarHelper;
import co.cask.cdap.proto.AdapterConfig;
import co.cask.cdap.templates.AdapterSpecification;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Files;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.reflect.TypeToken;
import org.apache.http.HttpResponse;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Type;
import java.util.List;
import java.util.Map;

/**
 * AdapterService life cycle tests.
 */
public class AdapterLifecycleTests extends AppFabricTestBase {
  private static final Gson GSON = new Gson();
  private static final Type ADAPTER_SPEC_LIST_TYPE =
    new TypeToken<List<AdapterSpecification>>() { }.getType();
  private static LocationFactory locationFactory;
  private static File adapterDir;
  private static AdapterService adapterService;

  @BeforeClass
  public static void setup() throws Exception {
    CConfiguration conf = getInjector().getInstance(CConfiguration.class);
    locationFactory = getInjector().getInstance(LocationFactory.class);
    adapterDir = new File(conf.get(Constants.AppFabric.APP_TEMPLATE_DIR));
    setupAdapter(DummyTemplate.class);
    adapterService = getInjector().getInstance(AdapterService.class);
    // this is called here because the service is already started by the test base at this po
    adapterService.registerTemplates();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    adapterService.stop();
  }

  @Test
  public void testAdapterLifeCycle() throws Exception {
    String namespaceId = Constants.DEFAULT_NAMESPACE;
    String adapterName = "myStreamConverter";
    DummyTemplate.Config config = new DummyTemplate.Config("somesource", "0 0 1 1 *");
    AdapterConfig adapterConfig = new AdapterConfig("description", DummyTemplate.NAME, GSON.toJsonTree(config));

    HttpResponse response = createAdapter(namespaceId, adapterName, adapterConfig);
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());

    // A duplicate create request (or any other create request with the same namespace + adapterName) will result in 409
    response = createAdapter(namespaceId, adapterName, adapterConfig);
    Assert.assertEquals(409, response.getStatusLine().getStatusCode());

    response = listAdapters(namespaceId);
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    List<AdapterSpecification> list = readResponse(response, ADAPTER_SPEC_LIST_TYPE);
    Assert.assertEquals(1, list.size());
    checkIsExpected(adapterConfig, list.get(0));

    response = getAdapter(namespaceId, adapterName);
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    AdapterSpecification receivedAdapterConfig = readResponse(response, AdapterSpecification.class);
    checkIsExpected(adapterConfig, receivedAdapterConfig);

    List<JsonObject> deployedApps = getAppList(namespaceId);
    Assert.assertEquals(1, deployedApps.size());
    JsonObject deployedApp = deployedApps.get(0);
    Assert.assertEquals(DummyTemplate.NAME, deployedApp.get("id").getAsString());

    response = getAdapterStatus(namespaceId, adapterName);
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    String status = readResponse(response);
    Assert.assertEquals("STOPPED", status);

    response = startStopAdapter(namespaceId, adapterName, "stop");
    Assert.assertEquals(409, response.getStatusLine().getStatusCode());

    response = startStopAdapter(namespaceId, adapterName, "start");
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());

    response = getAdapterStatus(namespaceId, adapterName);
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    status = readResponse(response);
    Assert.assertEquals("STARTED", status);

    response = deleteAdapter(namespaceId, adapterName);
    Assert.assertEquals(403, response.getStatusLine().getStatusCode());

    response = startStopAdapter(namespaceId, adapterName, "stop");
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());

    response = deleteAdapter(namespaceId, adapterName);
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());

    response = getAdapter(namespaceId, adapterName);
    Assert.assertEquals(404, response.getStatusLine().getStatusCode());
  }

  private void checkIsExpected(AdapterConfig config, AdapterSpecification spec) {
    Assert.assertEquals(config.getDescription(), spec.getDescription());
    Assert.assertEquals(config.getTemplate(), spec.getTemplate());
    Assert.assertEquals(config.getConfig(), spec.getConfig());
  }

  @Test
  public void testRestrictUserApps() throws Exception {
    // Testing that users can not deploy an application
    HttpResponse response = deploy(AppWithServices.class, DummyTemplate.NAME);
    Assert.assertEquals(400, response.getStatusLine().getStatusCode());
    String responseString = readResponse(response);
    Assert.assertTrue(String.format("Response String: %s", responseString),
                      responseString.contains("An ApplicationTemplate exists with a conflicting name."));


    // Users can not delete adapter applications
    response = doDelete(getVersionedAPIPath(String.format("apps/%s", DummyTemplate.NAME),
                                            Constants.Gateway.API_VERSION_3_TOKEN,
                                            Constants.DEFAULT_NAMESPACE));
    responseString = readResponse(response);
    Assert.assertTrue(String.format("Response String: %s", responseString),
                      responseString.contains("An ApplicationTemplate exists with a conflicting name."));
    Assert.assertEquals(400, response.getStatusLine().getStatusCode());
  }

  @Test
  public void testMissingTemplateReturns404() throws Exception {
    Map<String, Object> config = ImmutableMap.<String, Object>of("field1", "someval", "field2", "otherval");
    AdapterConfig badConfig = new AdapterConfig("description", "badtemplate", GSON.toJsonTree(config));
    HttpResponse response = createAdapter(Constants.DEFAULT_NAMESPACE, "badAdapter", badConfig);
    Assert.assertEquals(404, response.getStatusLine().getStatusCode());
  }

  @Test
  public void testInvalidJsonBodyReturns400() throws Exception {
    HttpResponse response = doPut(
      String.format("%s/namespaces/%s/adapters/%s",
                    Constants.Gateway.API_VERSION_3, Constants.DEFAULT_NAMESPACE, "myadapter"), "[]");
    Assert.assertEquals(400, response.getStatusLine().getStatusCode());
  }

  @Test
  public void testNoTemplateFieldReturns400() throws Exception {
    HttpResponse response = doPut(
      String.format("%s/namespaces/%s/adapters/%s",
                    Constants.Gateway.API_VERSION_3, Constants.DEFAULT_NAMESPACE, "myadapter"), "{}");
    Assert.assertEquals(400, response.getStatusLine().getStatusCode());
  }

  @Test
  public void testInvalidConfigReturns400() throws Exception {
    // TODO: implement once adapter creation calls configureTemplate()
  }

  @Test
  public void testDeployTemplate() throws Exception {
    HttpResponse response = doPut(
      String.format("%s/namespaces/%s/templates/%s",
                    Constants.Gateway.API_VERSION_3, Constants.DEFAULT_NAMESPACE, DummyTemplate.NAME), "{}");
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    ApplicationTemplateInfo info1 = adapterService.getApplicationTemplateInfo(DummyTemplate.NAME);
    response = doPut(
      String.format("%s/namespaces/%s/templates/%s",
                    Constants.Gateway.API_VERSION_3, Constants.DEFAULT_NAMESPACE, DummyTemplate.NAME), "{}");
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    ApplicationTemplateInfo info2 = adapterService.getApplicationTemplateInfo(DummyTemplate.NAME);
    Assert.assertNotEquals(info1.getDescription(), info2.getDescription());
  }

  private static void setupAdapter(Class<?> clz) throws IOException {
    Location adapterJar = AppJarHelper.createDeploymentJar(locationFactory, clz);
    File destination =  new File(String.format("%s/%s", adapterDir.getAbsolutePath(), adapterJar.getName()));
    Files.copy(Locations.newInputSupplier(adapterJar), destination);
  }

  private HttpResponse createAdapter(String namespaceId, String name, AdapterConfig config) throws Exception {
    return doPut(String.format("%s/namespaces/%s/adapters/%s",
                               Constants.Gateway.API_VERSION_3, namespaceId, name), GSON.toJson(config));
  }

  private HttpResponse getAdapterStatus(String namespaceId, String name) throws Exception {
    return doGet(String.format("%s/namespaces/%s/adapters/%s/status",
                               Constants.Gateway.API_VERSION_3, namespaceId, name));
  }

  private HttpResponse listAdapters(String namespaceId) throws Exception {
    return doGet(String.format("%s/namespaces/%s/adapters",
                               Constants.Gateway.API_VERSION_3, namespaceId));
  }

  private HttpResponse getAdapter(String namespaceId, String adapterId) throws Exception {
    return doGet(String.format("%s/namespaces/%s/adapters/%s",
                               Constants.Gateway.API_VERSION_3, namespaceId, adapterId));
  }

  private HttpResponse startStopAdapter(String namespaceId, String adapterId, String action) throws Exception {
    return doPost(String.format("%s/namespaces/%s/adapters/%s/%s",
                                Constants.Gateway.API_VERSION_3, namespaceId, adapterId, action));
  }

  private HttpResponse deleteAdapter(String namespaceId, String adapterId) throws Exception {
    return doDelete(String.format("%s/namespaces/%s/adapters/%s",
                                  Constants.Gateway.API_VERSION_3, namespaceId, adapterId));
  }
}
