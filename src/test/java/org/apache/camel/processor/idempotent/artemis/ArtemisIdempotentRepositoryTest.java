package org.apache.camel.processor.idempotent.artemis;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.activemq.artemis.api.core.client.ActiveMQClient;
import org.apache.activemq.artemis.api.core.client.ServerLocator;
import org.apache.activemq.artemis.core.server.embedded.EmbeddedActiveMQ;
import org.apache.camel.CamelExecutionException;
import org.apache.camel.LoggingLevel;
import org.apache.camel.RoutesBuilder;
import org.apache.camel.RuntimeCamelException;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.mock.MockEndpoint;
import org.apache.camel.test.junit4.CamelTestSupport;
import org.awaitility.Awaitility;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ArtemisIdempotentRepositoryTest extends CamelTestSupport {
  
  private static final Logger log = LoggerFactory.getLogger(ArtemisIdempotentRepositoryTest.class);

  private static final String ID_HEADER = "UniqueID";
  private static final String FAILURE_HEADER = "ForceFailure";
  private static final String BROKER_URL = "vm://0";
  private static final String ID_REPO_NAME = "test";
  
  private EmbeddedActiveMQ broker;
  private ArtemisIdempotentRepository idempotentRepository;
  private ArtemisIdempotentRepository idempotentRepository2;
  
  @Override
  protected void doPreSetup() throws Exception {
    super.doPreSetup();
    
    broker = new EmbeddedActiveMQ();
    log.info("Starting the Artemis server...");
    broker.start();
    
    ServerLocator serverLocator = ActiveMQClient.createServerLocator(BROKER_URL);
    idempotentRepository = new ArtemisIdempotentRepository(ID_REPO_NAME, serverLocator);
    
    idempotentRepository2 = new ArtemisIdempotentRepository(ID_REPO_NAME, serverLocator);
  }

  @Override
  protected void doPostTearDown() throws Exception {
    log.info("Stopping the Artemis server...");
    broker.stop();
    
    super.doPostTearDown();
  }

  @Override
  protected RoutesBuilder createRouteBuilder() throws Exception {
    return new RouteBuilder() {
      @Override
      public void configure() throws Exception {
        
        from("direct:invoke")
          .idempotentConsumer(header(ID_HEADER), idempotentRepository)
            .log(LoggingLevel.INFO, log, String.format("Processing message: route=[%s], key=[${headers.%s}]", "direct:invoke", ID_HEADER))
            .filter(header(FAILURE_HEADER))
              .throwException(RuntimeCamelException.class, String.format("The [%s] header is set to [true].", FAILURE_HEADER))
            .end()
            .to("mock:accepted")
          .end()
        ;
        
        from("direct:invoke2")
          .idempotentConsumer(header(ID_HEADER), idempotentRepository2)
            .log(LoggingLevel.INFO, log, String.format("Processing message: route=[%s], key=[${headers.%s}]", "direct:invoke2", ID_HEADER))
            .to("mock:accepted2")
          .end()
        ;
      }
    };
  }
  
  @Test
  public void testNoDuplicates() throws Exception {
    MockEndpoint mock = getMockEndpoint("mock:accepted");
    mock.expectedMessageCount(1);
    
    String message = "foo";
    String uniqueId = Thread.currentThread().getStackTrace()[1].getMethodName();
    Map<String, Object> headers = new HashMap<>();
    headers.put(ID_HEADER, uniqueId);
    template.sendBodyAndHeaders("direct:invoke", message, headers);
    template.sendBodyAndHeaders("direct:invoke", message, headers);
    
    MockEndpoint.assertIsSatisfied(mock);
  }
  
  @Test
  public void testNoDuplicatesDifferentRoute() throws Exception {
    MockEndpoint mock = getMockEndpoint("mock:accepted");
    mock.expectedMessageCount(1);

    MockEndpoint mock2 = getMockEndpoint("mock:accepted2");
    mock2.expectedMessageCount(0);
    
    String message = "foo";
    String uniqueId = Thread.currentThread().getStackTrace()[1].getMethodName();
    Map<String, Object> headers = new HashMap<>();
    headers.put(ID_HEADER, uniqueId);
    template.sendBodyAndHeaders("direct:invoke", message, headers);
    log.info("Waiting for 1 second so the idempotent repository can process/sync messages...");
    Awaitility.waitAtMost(1, TimeUnit.SECONDS).await();
    template.sendBodyAndHeaders("direct:invoke2", message, headers);
    
    MockEndpoint.assertIsSatisfied(mock, mock2);
  }
  
  @Test
  public void testFailureNoDuplicates() throws Exception {
    MockEndpoint mock = getMockEndpoint("mock:accepted");
    mock.expectedMessageCount(1);
    
    String message = "foo";
    String uniqueId = Thread.currentThread().getStackTrace()[1].getMethodName();
    Map<String, Object> headers = new HashMap<>();
    headers.put(ID_HEADER, uniqueId);
    headers.put(FAILURE_HEADER, true);
    try {
      template.sendBodyAndHeaders("direct:invoke", message, headers);
    } catch (CamelExecutionException e) {}
    headers.put(FAILURE_HEADER, false);
    template.sendBodyAndHeaders("direct:invoke", message, headers);
    
    MockEndpoint.assertIsSatisfied(mock);
  }
  
  @Test
  public void testFailureNoDuplicatesDifferentRoutes() throws Exception {
    MockEndpoint mock = getMockEndpoint("mock:accepted");
    mock.expectedMessageCount(0);

    MockEndpoint mock2 = getMockEndpoint("mock:accepted2");
    mock2.expectedMessageCount(1);
    
    String message = "foo";
    String uniqueId = Thread.currentThread().getStackTrace()[1].getMethodName();
    System.out.println("######################" + uniqueId);
    Map<String, Object> headers = new HashMap<>();
    headers.put(ID_HEADER, uniqueId);
    headers.put(FAILURE_HEADER, true);
    try {
      template.sendBodyAndHeaders("direct:invoke", message, headers);
    } catch (CamelExecutionException e) {}
    log.info("Waiting for 1 second so the idempotent repository can process/sync messages...");
    Awaitility.waitAtMost(1, TimeUnit.SECONDS).await();
    headers.put(FAILURE_HEADER, false);
    template.sendBodyAndHeaders("direct:invoke2", message, headers);
    
    MockEndpoint.assertIsSatisfied(mock);
  }
}
