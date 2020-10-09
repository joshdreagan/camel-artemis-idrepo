package org.apache.camel.processor.idempotent.artemis;

import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.QueueAttributes;
import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.client.ClientConsumer;
import org.apache.activemq.artemis.api.core.client.ClientMessage;
import org.apache.activemq.artemis.api.core.client.ClientProducer;
import org.apache.activemq.artemis.api.core.client.ClientSession;
import org.apache.activemq.artemis.api.core.client.ClientSession.QueueQuery;
import org.apache.activemq.artemis.api.core.client.ClientSessionFactory;
import org.apache.activemq.artemis.api.core.client.ServerLocator;
import org.apache.camel.RuntimeCamelException;
import org.apache.camel.spi.IdempotentRepository;
import org.apache.camel.support.ServiceSupport;
import org.apache.camel.util.LRUCacheFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ArtemisIdempotentRepository extends ServiceSupport implements IdempotentRepository<String> {

  private static final Logger log = LoggerFactory.getLogger(ArtemisIdempotentRepository.class);
  
  public static final String MESSAGE_ORIGIN_HEADER = "ArtemisIdempotentRepositoryMessageOrigin";
  public static final String MESSAGE_TYPE_HEADER = "ArtemisIdempotentRepositoryMessageType";

  private static final int DEFAULT_CACHE_SIZE = 1000;
  private static final byte EMPTY_CACHE_VALUE = (byte) 0;

  private final String repoId;
  private final ServerLocator serverLocator;
  private final int maxCacheSize;

  private String username;
  private String password;

  private final String _selfUuid = UUID.randomUUID().toString();;
  private final Map<String, Object> _cache;

  private ClientSessionFactory _factory;
  private ClientSession _session;
  private ClientProducer _producer;

  public ArtemisIdempotentRepository(String repoId, ServerLocator serverLocator) {
    this(repoId, serverLocator, DEFAULT_CACHE_SIZE);
  }

  public ArtemisIdempotentRepository(String repoId, ServerLocator serverLocator, int maxCacheSize) {
    if (Objects.requireNonNull(repoId, "The 'repoId' parameter must not be null.").trim().isEmpty()) {
      throw new IllegalArgumentException("The 'repoId' parameter must not be empty.");
    }
    Objects.requireNonNull(serverLocator, "The 'serverLocator' parameter must not be null.");
    if (maxCacheSize <= 0) {
      throw new IllegalArgumentException("The 'maxCacheSize' parameter must be greater than 0.");
    }

    this.repoId = repoId;
    this.serverLocator = serverLocator;
    this.maxCacheSize = maxCacheSize;
    this._cache = LRUCacheFactory.newLRUCache(maxCacheSize);
    
    log.debug(String.format("Created %s instance: [%s]", this.getClass().getSimpleName(), this._selfUuid));
  }

  public String getRepoId() {
    return repoId;
  }

  public ServerLocator getServerLocator() {
    return serverLocator;
  }

  public int getMaxCacheSize() {
    return maxCacheSize;
  }

  public String getUsername() {
    return username;
  }

  public void setUsername(String username) {
    this.username = username;
  }

  public String getPassword() {
    return password;
  }

  public void setPassword(String password) {
    this.password = password;
  }

  @Override
  protected void doStop() throws Exception {
    _session.close();
    _factory.close();
  }

  @Override
  protected void doStart() throws Exception {
    _factory = serverLocator.createSessionFactory();
    _session = _factory.createSession(username, password, false, true, true, serverLocator.isPreAcknowledge(), serverLocator.getAckBatchSize());

    QueueQuery queueQuery = _session.queueQuery(new SimpleString(repoId));
    if (!queueQuery.isExists()) {
      log.debug(String.format("Ring queue [%s] did not exist. Creating it.", repoId));
      QueueAttributes queueAttributes = new QueueAttributes();
      queueAttributes.setRoutingType(RoutingType.ANYCAST);
      queueAttributes.setRingSize((long) maxCacheSize);
      queueAttributes.setNonDestructive(true);
      queueAttributes.setDurable(true);
      queueAttributes.setMaxConsumers(-1);
      queueAttributes.setPurgeOnNoConsumers(false);
      queueAttributes.setAutoDelete(false);
      _session.createQueue(new SimpleString(repoId), new SimpleString(repoId), true, queueAttributes);
    } else {
      log.debug(String.format("Ring queue [%s] already exists. Skipping creation.", repoId));
    }

    _producer = _session.createProducer(repoId);
    ClientConsumer consumer = _session.createConsumer(repoId, String.format("%1$s IS NULL OR %1$s <> '%2$s'", MESSAGE_ORIGIN_HEADER, _selfUuid));
    _session.start();

    log.debug("Warming up cache...");
    ClientMessage syncMessage = null;
    do {
      syncMessage = consumer.receiveImmediate();
      handleMessage(syncMessage);
    } while (syncMessage != null);
    log.debug("Done warming up cache.");

    consumer.setMessageHandler((ClientMessage message) -> {
      try {
        handleMessage(message);
      } catch (ActiveMQException e) {
        throw new RuntimeCamelException(e);
      }
    });
  }

  private void handleMessage(ClientMessage message) throws ActiveMQException {
    if (message != null) {
      byte messageTypeOrdinal = message.getByteProperty(MESSAGE_TYPE_HEADER);
      if (messageTypeOrdinal > MessageType.values().length) {
        log.debug(String.format("Unknown message type ordinal: [%s]. Dropping message.", message.getByteProperty(MESSAGE_TYPE_HEADER)));
        return;
      }
      MessageType type = MessageType.values()[messageTypeOrdinal];
      String body = null;
      switch (type) {
        case ADD:
          body = message.getBodyBuffer().readString();
          log.debug(String.format("Processing %s message: [%s].", type, body));
          _cache.put(body, EMPTY_CACHE_VALUE);
          break;
        case CLEAR:
          log.debug(String.format("Processing %s message.", type));
          _cache.clear();
          break;
        case REMOVE:
          body = message.getBodyBuffer().readString();
          log.debug(String.format("Processing %s message: [%s].", type, body));
          _cache.remove(body);
          break;
        default:
          log.debug(String.format("Unknown message type: [%s].", type));
          break;
      }
      message.acknowledge();
    } else {
      log.debug("Message was null. Ignoring.");
    }
  }

  @Override
  public boolean add(String key) {
    if (!_cache.containsKey(key)) {
      try {
        log.debug(String.format("Adding key to cache: [%s].", key));
        broadcastAction(MessageType.ADD, key);
        _cache.put(key, EMPTY_CACHE_VALUE);
        return true;
      } catch (ActiveMQException e) {
        log.debug(String.format("Error adding key to cache: [%s].", key), e);
        return false;
      }
    }
    log.debug(String.format("Cache already contains key: [%s]. Skipping.", key));
    return false;
  }

  @Override
  public void clear() {
    try {
      log.debug("Clearing cache.");
      broadcastAction(MessageType.CLEAR, null);
      _cache.clear();
    } catch (ActiveMQException e) {
      log.debug("Error clearing cache.", e);
    }
  }

  @Override
  public boolean contains(String key) {
    boolean contains = _cache.containsKey(key);
    log.debug(String.format("Cache %s key: [%s].", (contains)?"contains":"does not contain", key));
    return contains;
  }

  @Override
  public boolean confirm(String key) {
    return true;
  }

  @Override
  public boolean remove(String key) {
    if (_cache.containsKey(key)) {
      try {
        log.debug(String.format("Removing key from cache: [%s].", key));
        broadcastAction(MessageType.REMOVE, key);
        _cache.remove(key);
        return true;
      } catch (ActiveMQException e) {
        log.debug(String.format("Error removing key from cache: [%s].", key), e);
        return false;
      }
    }
    log.debug(String.format("Cache does not contain key: [%s]. Skipping.", key));
    return false;
  }

  private void broadcastAction(MessageType type, String key) throws ActiveMQException {
    log.debug(String.format("Broadcasting action: type=[%s], key=[%s].", type, key));
    ClientMessage message = _session.createMessage(true);
    message.putStringProperty(MESSAGE_ORIGIN_HEADER, _selfUuid);
    message.putByteProperty(MESSAGE_TYPE_HEADER, (byte) type.ordinal());
    if (key != null) {
      message.getBodyBuffer().writeString(key);
    }
    _producer.send(message);
  }

  private enum MessageType {
    ADD,
    CLEAR,
    REMOVE
  }
}
