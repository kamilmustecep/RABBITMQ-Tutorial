﻿using Microsoft.Extensions.DependencyInjection;
using RABBITMQ_TUTORIAL.CONSUMER.Services;

var serviceProvider = new ServiceCollection()
                .AddSingleton<IRabbitMQConsumer, RabbitMQConsumer>()
                .BuildServiceProvider();

var consumer = serviceProvider.GetService<IRabbitMQConsumer>();


//Personel queue consuming
consumer.ConsomeSpecialMessageWithUserId(37,"Kuyruk37");

//General queue consuming
//consumer.StartConsuming("Kuyruk37");

Console.WriteLine("RabbitMQ Consumer is listening. Press any key to exit.");
Console.ReadKey();

consumer.StopConsuming();