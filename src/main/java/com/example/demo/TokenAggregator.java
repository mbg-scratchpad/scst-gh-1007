/*
 * Copyright 2017 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.example.demo;

import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.integration.annotation.Aggregator;
import org.springframework.integration.annotation.CorrelationStrategy;
import org.springframework.integration.annotation.MessageEndpoint;
import org.springframework.integration.annotation.ReleaseStrategy;
import org.springframework.integration.support.MessageBuilder;
import org.springframework.messaging.Message;
import org.springframework.messaging.handler.annotation.Headers;

import static com.example.demo.ChannelBridgeConfig.TOKEN_READ_BRIDGE;

/**
 * @author Marius Bogoevici
 */
@MessageEndpoint
public class TokenAggregator {

	private static final Logger LOGGER = LoggerFactory.getLogger(TokenAggregator.class);

	@Aggregator(
			inputChannel = TOKEN_READ_BRIDGE,
			outputChannel = Channels.TOKEN_WRITE,
			discardChannel = Channels.DISCARD_WRITE
	)
	public Message<AggregatedToken> aggregate(List<Message<Token>> messages, @Headers Map<String, Object> headers) {
		LOGGER.info("Aggregating ... ");
		StringBuffer stringBuffer = new StringBuffer();
		for (Message<Token> message : messages) {
			stringBuffer.append(message.getPayload().getData());
		}
		Message<AggregatedToken> returnMessage = MessageBuilder
														 .withPayload(new AggregatedToken(stringBuffer.toString()))
														 .build();
		LOGGER.info("Sending ... " + returnMessage);
		return returnMessage;
	}

	@CorrelationStrategy
	public int correlate(Message<Token> message) {
		return Integer.parseInt(message.getPayload().getGroupId());
	}

	@ReleaseStrategy
	public boolean release(final List<Message<Token>> messages) {
		LOGGER.info("Checking whether to release... ");
		return messages.size() >= 1;
	}
}
