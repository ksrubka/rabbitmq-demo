package com.example.demo;

import com.example.demo.RabbitMqManager.ChannelCallable;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.AMQP.Exchange.DeclareOk;
import com.rabbitmq.client.Channel;

import javax.inject.Inject;
import java.io.IOException;
import java.util.Map;
import java.util.UUID;

public class UserMessageManager {

    static final String USER_INBOXES_EXCHANGE = "user-inboxes";
    static final String MESSAGE_CONTENT_TYPE = "application/vnd.ccm.pmsg.v1+json";
    static final String MESSAGE_ENCODING = "UTF-8";

    @Inject
    RabbitMqManager rabbitMqManager;

    public void onApplicationStart() {
        rabbitMqManager.call(new ChannelCallable<DeclareOk>() {
            @Override
            public String getDescription() {
                return "Declaring direct exchange: " +
                        USER_INBOXES_EXCHANGE;
            }

            @Override
            public DeclareOk call(final Channel channel) throws IOException {
                String exchange = USER_INBOXES_EXCHANGE;
                String type = "direct";

                // survive a server restart
                boolean durable = true;

                // keep it even if nobody is using it
                boolean autoDelete = false;

                // no special arguments
                Map<String, Object> arguments = null;
                return channel.exchangeDeclare(exchange, type, durable, autoDelete, arguments);
            }
        });
    }

    public void onUserLogin(final long userId) {
        final String queue = getUserInboxQueue(userId);
        rabbitMqManager.call(new ChannelCallable<AMQP.Queue.BindOk>() {
            @Override
            public String getDescription() {
                return "Declaring user queue: " + queue + ",binding it to exchange: "
                        + USER_INBOXES_EXCHANGE;
            }

            @Override
            public AMQP.Queue.BindOk call(final Channel channel) throws IOException {
                return declareUserMessageQueue(queue, channel);
            }
        });
    }

    private String getUserInboxQueue(long userId) {
        return "user-inbox." + userId;
    }

    private AMQP.Queue.BindOk declareUserMessageQueue(final String queue, final Channel channel) throws IOException {
        // survive a server restart
        boolean durable = true;

        // keep the queue
        boolean autoDelete = false;

        // can be consumed by another connection
        boolean exclusive = false;

        // no special arguments
        Map<String, Object> arguments = null;
        channel.queueDeclare(queue, durable, exclusive, autoDelete, arguments);

        // bind the addressee's queue to the direct exchange
        String routingKey = queue;
        return channel.queueBind(queue, USER_INBOXES_EXCHANGE, routingKey);
    }

    public String sendUserMessage(final long userId, final String jsonMessage) {
        return rabbitMqManager.call(new ChannelCallable<String>() {
            @Override
            public String getDescription() {
                return "Sending message to user: " + userId;
            }

            @Override
            public String call(final Channel channel)
                    throws IOException {
                String queue = getUserInboxQueue(userId);

                // it may not exist so declare it
                declareUserMessageQueue(queue, channel);
                String messageId = UUID.randomUUID().toString();
                AMQP.BasicProperties props = new AMQP.BasicProperties.Builder()
                        .contentType(MESSAGE_CONTENT_TYPE)
                        .contentEncoding(MESSAGE_ENCODING)
                        .messageId(messageId)
                        .deliveryMode(2)
                        .build();
                String routingKey = queue;

                // publish the message to the direct exchange
                channel.basicPublish(USER_INBOXES_EXCHANGE,
                        routingKey, props,
                        jsonMessage.getBytes(MESSAGE_ENCODING));
                return messageId;
            }
        });
    }
}