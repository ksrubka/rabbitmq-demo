package com.example.demo;

import com.rabbitmq.client.ConnectionFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

@SpringBootApplication
public class RabbitmqDemoApplication {

	public static void main(String[] args) {
		SpringApplication.run(RabbitmqDemoApplication.class, args);
	}

	@Bean
	public RabbitMqManager rabbitMqManager() {
		ConnectionFactory factory = new ConnectionFactory();
		factory.setUsername("bilo-dev");
		factory.setPassword("amqp8!");
		factory.setVirtualHost("bilo-dev-vhost");
		factory.setHost("localhost");
		factory.setPort(5672);
		RabbitMqManager connectionManager = new RabbitMqManager(factory);
		connectionManager.start();
		return connectionManager;
	}
}
