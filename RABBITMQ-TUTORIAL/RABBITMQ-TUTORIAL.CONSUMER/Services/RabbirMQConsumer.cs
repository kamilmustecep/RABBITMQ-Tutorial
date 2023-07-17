using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace RABBITMQ_TUTORIAL.CONSUMER.Services
{
    public class RabbitMQConsumer : IRabbitMQConsumer
    {
        private readonly IConnection _connection;
        private readonly IModel _channel;

        public RabbitMQConsumer()
        {
            var factory = new ConnectionFactory
            {
                HostName = "localhost", // RabbitMQ sunucusunun adresini burada belirtin
                UserName = "guest", // RabbitMQ kullanıcı adını burada belirtin
                Password = "guest" // RabbitMQ şifresini burada belirtin
            };

            _connection = factory.CreateConnection();
            _channel = _connection.CreateModel();
        }

        public void StartConsuming(string queueName)
        {
            _channel.QueueDeclare(queue: queueName, durable: false, exclusive: false, autoDelete: false, arguments: null);

            var consumer = new EventingBasicConsumer(_channel);
            consumer.Received += (model, ea) =>
            {
                var message = Encoding.UTF8.GetString(ea.Body.ToArray());
                Console.WriteLine($"Received message: {message}");

                // Veriyi işleme işlemlerini burada gerçekleştirin

                _channel.BasicAck(ea.DeliveryTag, false);
            };

            _channel.BasicConsume(queue: queueName, autoAck: false, consumer: consumer);
        }

        public void StopConsuming()
        {
            _channel.Close();
            _connection.Close();
        }
    }
}
