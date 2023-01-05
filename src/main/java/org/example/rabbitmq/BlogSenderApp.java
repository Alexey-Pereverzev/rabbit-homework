package org.example.rabbitmq;

import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.util.Scanner;

public class BlogSenderApp {        // Sender
    private static final String EXCHANGE_NAME = "BlogApp";

    public static void main(String[] argv) throws Exception {
        Scanner in = new Scanner(System.in);
        String message = "";
        String delimiter = " ";
        String[] result;
        String header = "";
        String body = "";
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        try (Connection connection = factory.newConnection();
             Channel channel = connection.createChannel())
        {
            channel.exchangeDeclare(EXCHANGE_NAME, BuiltinExchangeType.DIRECT);
            boolean running = true;
            while (running) {
                System.out.print("Write a blog post (for exit type 'exit'): ");
                message = in.nextLine();
                if (message.equals("exit")) {
                    running = false;
                } else {
                    result = message.split(delimiter,2);
                    if (result.length>1) {
                        header = result[0];
                        body = result[1];
                        channel.basicPublish(EXCHANGE_NAME, header, null, body.getBytes("UTF-8"));
                        System.out.println("Message is sent with topic: " + header);
                    }
                }
            }
        }

    }


}
