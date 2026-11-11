package ir.sharifi.confirmbench;

import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.DirectExchange;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.boot.autoconfigure.amqp.RabbitProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class RabbitSetup {

	static final String EXCHANGE = "bench.confirm";

	static final String QUEUE = "bench.confirm";

	static final String ROUTING_KEY = "bench";

	@Bean
	CachingConnectionFactory rabbitConnectionFactory(RabbitProperties properties) {
		CachingConnectionFactory factory = new CachingConnectionFactory(properties.getHost(), properties.getPort());
		factory.setUsername(properties.getUsername());
		factory.setPassword(properties.getPassword());
		String virtualHost = properties.getVirtualHost();
		factory.setVirtualHost(virtualHost == null || virtualHost.isBlank() ? "/" : virtualHost);
		factory.setPublisherConfirmType(properties.getPublisherConfirmType());
		factory.setPublisherReturns(properties.isPublisherReturns());
		factory.setChannelCacheSize(1);
		return factory;
	}

	@Bean
	RabbitTemplate rabbitTemplate(CachingConnectionFactory rabbitConnectionFactory) {
		RabbitTemplate template = new RabbitTemplate(rabbitConnectionFactory);
		template.setUsePublisherConnection(true);
		template.setChannelTransacted(false);
		template.setMandatory(true);
		return template;
	}

	@Bean
	DirectExchange benchExchange() {
		return new DirectExchange(EXCHANGE, true, false);
	}

	@Bean
	Queue benchQueue() {
		return new Queue(QUEUE, true);
	}

	@Bean
	Binding benchBinding(Queue benchQueue, DirectExchange benchExchange) {
		return BindingBuilder.bind(benchQueue).to(benchExchange).with(ROUTING_KEY);
	}
}
