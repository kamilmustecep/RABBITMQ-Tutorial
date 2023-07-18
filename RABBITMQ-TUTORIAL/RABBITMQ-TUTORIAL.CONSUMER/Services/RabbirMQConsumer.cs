using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using RABBITMQ_TUTORIAL.Service;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Channels;
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
                    Console.WriteLine($"Received general message: {message}");

                    // Veriyi işleme işlemlerini burada gerçekleştirin

                    _channel.BasicAck(ea.DeliveryTag, false);
            };

            _channel.BasicConsume(queue: queueName, autoAck: false, consumer: consumer);
        }

        public void ConsomeSpecialMessageWithUserId(int user_id, string queueName)
        {

            _channel.QueueDeclare(queue: queueName, durable: false, exclusive: false, autoDelete: false, arguments: null);

            var consumer = new EventingBasicConsumer(_channel);


            /*

               - BasicQos(0, 1, false) PARAMETRELERİ ;

               prefetchSize (uint): Bu parametre, bir mesajın maksimum boyutunu belirtir. Yani, bu boyutu aşan bir mesaj alındığında bu mesajın tüketiciye verilmesi engellenir.
                                    Genellikle bu değer 0 olarak ayarlanır, bu da mesaj boyutunun önemsiz olduğu anlamına gelir ve mesaj boyutu kontrolü yapılmaz.
                                    (200 * 1024; // 200 KB)

               prefetchCount (ushort): Bu parametre, tüketiciye eşzamanlı olarak kaç mesajın verileceğini belirtir.
                                       Örneğin, prefetchCount değeri 1 olarak ayarlanırsa, tüketiciye sırayla yalnızca bir mesaj gönderilir ve o mesaj işlenene kadar diğer mesajlar tüketiciye gönderilmez.

               global (bool): Bu parametre, tüketici için prefetchCount değerinin kümesinin, kanal (channel) düzeyinde mi yoksa tüketici (consumer) düzeyinde mi kullanılacağını belirler.
                              Eğer global değeri true olarak ayarlanırsa, tüm tüketicilere aynı prefetchCount değeri uygulanır.
                              Eğer false olarak ayarlanırsa, her tüketiciye ayrı bir prefetchCount değeri uygulanır.

                */

            //_channel.BasicQos(0, 1, false);


            consumer.Received += (model, ea) =>
            {
                var message = Encoding.UTF8.GetString(ea.Body.ToArray());

                if (ea.BasicProperties.Headers != null)
                {
                    // Mesajın kullanıcıya ait olduğunu doğrulayalım
                    if (ea.BasicProperties.Headers.TryGetValue("user_id", out object userIdObj))
                    {
                        int userId = Convert.ToInt32(Encoding.UTF8.GetString((byte[])userIdObj));
                        if (userId == user_id)
                        {
                            Console.WriteLine(user_id + " id'li kişi için gelen kişisel message tüketildi : " + message);

                            /*

                            BasicAck metodunun multiple parametresi, mesajları topluca onaylamak için kullanılır.
                            Bu parametre, true olarak ayarlandığında, belirtilen deliveryTag değerinden küçük veya eşit olan tüm mesajlar onaylanır.
                            Yani, multiple değeri true olarak ayarlandığında, belirtilen deliveryTag'den önceki tüm mesajlar başarıyla işlendi olarak kabul edilir
                            ve kuyruktan çıkarılır.

                             */

                            _channel.BasicAck(deliveryTag: ea.DeliveryTag, multiple: false);
                        }
                        else
                        {
                            Console.WriteLine("User id'si "+ userId + " olan kişiye ait mesaj : " + message);
                        }
                    }
                    
                }
                else
                {
                    Console.WriteLine("Header NULL olan mesaj : " + message);
                }

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
