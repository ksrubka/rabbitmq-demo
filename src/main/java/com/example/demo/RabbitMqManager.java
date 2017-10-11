package com.example.demo;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.ShutdownListener;
import com.rabbitmq.client.ShutdownSignalException;

import java.io.IOException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

public class RabbitMqManager implements ShutdownListener {

    private final static Logger LOGGER =
            Logger.getLogger(RabbitMqManager.class.getName());
    private final ConnectionFactory factory;
    private final ScheduledExecutorService executor;
    private volatile Connection connection;

    public RabbitMqManager(final ConnectionFactory factory) {
        this.factory = factory;
        executor = Executors.newSingleThreadScheduledExecutor();
        connection = null;
    }

    public interface ChannelCallable<T> {
        String getDescription();

        T call(Channel channel) throws IOException;
    }

    public <T> T call(final ChannelCallable<T> callable) {
        final Channel channel = createChannel();
        if (channel != null) {
            try {
                return callable.call(channel);
            } catch (final Exception e) {
                LOGGER.log(Level.SEVERE, "Failed to run: " +
                        callable.getDescription() + " on channel: " +
                        channel, e);
            } finally {
                closeChannel(channel);
            }
        }
        return null;
    }

    @Override
    public void shutdownCompleted(ShutdownSignalException cause) {
        // reconnect only on unexpected errors
        if (!cause.isInitiatedByApplication()) {
            LOGGER.log(Level.SEVERE, "Lost connection to " +
                            factory.getHost() + ":" + factory.getPort(),
                    cause);
            connection = null;
            asyncWaitAndReconnect();
        }
    }

    public void start() {
        try {
            connection = factory.newConnection();
            connection.addShutdownListener(this);
            LOGGER.info("Connected to " + factory.getHost() + ":" +
                    factory.getPort());
        } catch (final Exception e) {
            LOGGER.log(Level.SEVERE, "Failed to connect to " +
                    factory.getHost() + ":" + factory.getPort(), e);
            asyncWaitAndReconnect();
        }
    }

    public void stop() {
        executor.shutdownNow();
        if (connection == null) {
            return;
        }
        try {
            connection.close();
        } catch (final Exception e) {
            LOGGER.log(Level.SEVERE, "Failed to close connection", e);
        } finally {
            connection = null;
        }
    }

    public Channel createChannel() {
        try {
            return connection == null ? null : connection.createChannel();
        } catch (final Exception e) {
            LOGGER.log(Level.SEVERE, "Failed to create channel", e);
            return null;
        }
    }

    public void closeChannel(final Channel channel) {
        // isOpen is not fully trustable!
        if ((channel == null) || (!channel.isOpen())) {
            return;
        }
        try {
            channel.close();
        } catch (final Exception e) {
            LOGGER.log(Level.SEVERE, "Failed to close channel: " +
                    channel, e);
        }
    }

    private void asyncWaitAndReconnect() {
        executor.schedule((Runnable) () -> start(), 15, TimeUnit.SECONDS);
    }
}