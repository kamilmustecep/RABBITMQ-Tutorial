using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System.Text;
using System.Threading.Channels;

namespace RABBITMQ_TUTORIAL.Service
{
    public class RabbitMQService : IRabbitMQService, IDisposable
    {
        private readonly IConnection _connection;
        private IModel _channel;
        private string _consumerTag;

        public RabbitMQService()
        {
            var factory = new ConnectionFactory()
            {
                HostName = "localhost", // RabbitMQ sunucusunun adresini burada belirtin
                UserName = "guest", // RabbitMQ kullanıcı adını burada belirtin
                Password = "guest" // RabbitMQ şifresini burada belirtin
            };

            _connection = factory.CreateConnection();
            _channel = _connection.CreateModel();
        }

        public void PublishMessage(string queueName, string message)
        {
            using (var channel = _connection.CreateModel())
            {

                /*
                 
                • queue: Kuyruk adı. Mesajların gönderileceği veya alınacağı kuyruğun adını belirtir.
                • durable: Kalıcılık durumu. Eğer true olarak ayarlanırsa, kuyruk ve mesajlar disk üzerinde kalıcı olarak saklanır. Varsayılan değeri false'dur.
                • exclusive: Özel durum. Eğer true olarak ayarlanırsa, yalnızca bu bağlantıya özgü olacak şekilde kuyruk oluşturulur. Varsayılan değeri false'dur.
                • autoDelete: Otomatik silme. Eğer true olarak ayarlanırsa, tüm tüketiciler bağlantıyı kapattığında veya son mesaj alındığında kuyruk otomatik olarak silinir. Varsayılan değeri false'dur.
                • arguments: İsteğe bağlı argümanlar. Kuyruğa özel ek argümanlar sağlamak için kullanılır. Genellikle kullanılmaz, bu nedenle varsayılan değeri null'dır.
                
                1-ARGUMENTS Komutları;

                "x-message-ttl" (int): Bir mesajın kuyrukta kalabileceği maksimum süreyi belirtir. Örneğin, 60000, mesajların 60 saniye boyunca kuyrukta kalmasını sağlar.
                "x-expires" (int): Kuyruğun otomatik olarak silineceği süreyi belirtir (milisaniye cinsinden). Örneğin, 60000, kuyruğun 60 saniye boyunca inaktif kaldıktan sonra otomatik olarak silinmesini sağlar.
                "x-max-priority" (byte): Kuyrukta desteklenen maksimum öncelik seviyesini belirtir. Örneğin, 10, kuyruğun 10 farklı öncelik seviyesini desteklediğini gösterir.
                "x-queue-mode" (string): Kuyruğun modunu belirtir. "lazy" olarak ayarlandığında, mesajlar bellekte daha az yer kaplayan bir şekilde tutulur ve disk üzerinde depolanır. "default" ise normal modu temsil eder.
                "x-dead-letter-exchange" (string): Mesajın işlenemediği durumlarda yönlendirileceği değişim adını belirtir. Örneğin, "my_dead_letter_exchange".
                "x-dead-letter-routing-key" (string): Mesajın işlenemediği durumlarda yönlendirileceği yönlendirme anahtarını belirtir. Örneğin, "my_dead_letter_routing_key".
                "x-max-length" (int): Kuyruktaki maksimum mesaj sayısını belirtir. Örneğin, 100, kuyrukta en fazla 100 mesaj tutulmasını sağlar.
                "x-overflow" (string): Kuyruk dolu olduğunda uygulanacak aşım davranışını belirtir. "reject-publish" olarak ayarlandığında, yeni mesajlar reddedilir. "drop-head" olarak ayarlandığında, en eski mesaj kuyruktan düşer.
                "x-queue-master-locator" (string): Kuyruk kümesi (cluster) senaryolarında, kuyruğun ana düğümünü belirtir. "min-masters" olarak ayarlandığında, kuyruk en az sayıda ana düğümde oluşturulur.


                2-ARGUMENTS Kullanım: 

                var arguments = new Dictionary<string, object>
                {
                    { "x-message-ttl", 60000 } // 60 saniye
                };


                 */

                channel.QueueDeclare(queue: queueName, durable: false, exclusive: false, autoDelete: false, arguments: null);

                var body = Encoding.UTF8.GetBytes(message);

                /*
                 
                • exchange: Mesajların gönderileceği değişim adı. Boş bir dize ("") kullanarak varsayılan değişimi belirtebilirsiniz.
                • routingKey: Mesajın yönlendirileceği kuyruk adı veya değişim anahtarı. Mesaj, belirtilen kuyruğa veya değişim anahtarına yönlendirilir.
                • body: Mesajın içeriği. Gönderilecek mesajın veri içeriğini temsil eder. Genellikle bir byte dizisi olarak sağlanır.
                • basicProperties: Temel özellikler. Mesaja ek özellikler eklemek için kullanılır. Örneğin, mesajın öncelik seviyesini veya zaman aşımını belirlemek için kullanılabilir. Varsayılan olarak null kullanılır.
               

                1-BASICPROPERTIES Komutları;

                Priority (byte): Mesajın öncelik seviyesini belirtir. Daha yüksek bir öncelik değeri, mesajın daha önce işlenmesini sağlar. Örneğin, 1, en düşük öncelik seviyesini temsil eder.
                DeliveryMode (byte): Mesajın teslimat modunu belirtir. 1, mesajın kalıcı (persistent) olduğunu belirtirken, 2, mesajın geçici (transient) olduğunu belirtir.
                Expiration (string): Mesajın süresini belirtir. Mesaj, belirtilen süre sonunda kuyruktan otomatik olarak silinir. Örneğin, "60000", mesajın 60 saniye sonra kuyruktan silinmesini sağlar.
                ContentType (string): Mesaj içeriğinin MIME tipini belirtir. Örneğin, "application/json", mesajın JSON formatında olduğunu belirtir.
                ReplyTo (string): Yanıtın yönlendirileceği kuyruğun adını belirtir. Örneğin, "my_reply_queue".

                1-BASICPROPERTIES Kullanımı;

                var properties = channel.CreateBasicProperties();
                properties.Priority = 1; // Öncelik seviyesi 1

                 */


                channel.BasicPublish(exchange: "", routingKey: queueName, basicProperties: null, body: body);
            }
        }

        public void ProduceSpecialMessageWithUserId(int userId, string queueName, string message)
        {


            using (var channel = _connection.CreateModel())
            {
                // Kuyruk oluşturma (varsa tekrar oluşturmaz)

                /*
                 
                 durable (dayanıklılık) özelliği, RabbitMQ'da kuyrukların kalıcı olup olmadığını belirler.
                 Bir kuyruk dayanıklı (durable = true) olarak tanımlandığında, kuyruk mesajları fiziksel olarak diskte saklanır ve sunucu yeniden başlatıldığında bile kuyruk ve mesajlar korunur.
                 Eğer durable = false olarak tanımlanırsa, kuyruk geçici olur ve sunucu yeniden başlatıldığında kuyruk ve mesajlar kaybolabilir.
                 
                 */

                channel.QueueDeclare(queue: queueName, durable: false, exclusive: false, autoDelete: false, arguments: null);

                // PDF içeriğini byte dizisine dönüştür
                byte[] pdfBytes = Encoding.UTF8.GetBytes(message);

                // Kullanıcı kimliğini header'a ekleyerek mesajı kuyruğa ekle
                var properties = channel.CreateBasicProperties();
                properties.Persistent = true; // Mesajların kalıcı olması için
                properties.Headers = new System.Collections.Generic.Dictionary<string, object>
                {
                    { "user_id", userId.ToString() }
                };

                channel.BasicPublish(exchange: "", routingKey: queueName, basicProperties: properties, body: pdfBytes);
            }
        }

        public void ConsomeSpecialMessageWithUserId(int user_id, string queueName)
        {

            using (var channel = _connection.CreateModel())
            {

                // Kullanıcıya özel mesajı tüketme işlemi
                // Kullanıcı kimliği "user_id" değişkeniyle temsil ediliyor.
                // Kullanıcının sadece kendi verilerini tüketmesini sağlamak için kullanıcı kimliğiyle mesajları filtreleyeceğiz.


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

                channel.BasicQos(0, 1, false);
                var consumer = new EventingBasicConsumer(channel);
                consumer.Received += (model, ea) =>
                {
                    // Mesajın kullanıcıya ait olduğunu doğrulayalım
                    if (ea.BasicProperties.Headers.TryGetValue("user_id", out object userIdObj))
                    {
                        int userId = Convert.ToInt32(Encoding.UTF8.GetString((byte[])userIdObj));
                        if (userId == user_id)
                        {
                            var message = Encoding.UTF8.GetString(ea.Body.ToArray());
                            Console.WriteLine("Kişisel message tüketildi : " + message);

                            /*
                             
                            BasicAck metodunun multiple parametresi, mesajları topluca onaylamak için kullanılır.
                            Bu parametre, true olarak ayarlandığında, belirtilen deliveryTag değerinden küçük veya eşit olan tüm mesajlar onaylanır.
                            Yani, multiple değeri true olarak ayarlandığında, belirtilen deliveryTag'den önceki tüm mesajlar başarıyla işlendi olarak kabul edilir
                            ve kuyruktan çıkarılır.
                             
                             */

                            channel.BasicAck(deliveryTag: ea.DeliveryTag, multiple: false);
                        }
                    }
                };

                // Kuyruğu dinleme
                channel.BasicConsume(queue: queueName, autoAck: false, consumer: consumer);

                Console.WriteLine("Kullanıcıya özel mesajları tüketmek için bekleniyor...");
                Console.ReadLine();
            }



        }

        public List<string> GetMessagesFromQueue(string queueName)
        {
            var messages = new List<string>();

            using (var channel = _connection.CreateModel())
            {
                var messageCount = GetMessageCount(channel, queueName);

                for (int i = 0; i < messageCount; i++)
                {
                    var message = channel.BasicGet(queue: queueName, autoAck: true);

                    if (message != null)
                    {
                        var body = message.Body.ToArray();
                        var messageContent = Encoding.UTF8.GetString(body);
                        messages.Add(messageContent);
                    }
                }
            }

            return messages;
        }

        public int GetMessageCount(IModel channel, string queueName)
        {
            var queueDeclareOk = channel.QueueDeclarePassive(queueName);
            return (int)queueDeclareOk.MessageCount;
        }


        //Belirtilen bir exchange oluşturur.
        public void CreateExchange(string exchangeName, string exchangeType)
        {
            using (var channel = _connection.CreateModel())
            {
                channel.ExchangeDeclare(exchange: exchangeName, type: exchangeType);
            }
        }


        //Bir kuyruğu belirtilen değişime ve yönlendirme anahtarına bağlar.
        public void BindQueue(string queueName, string exchangeName, string routingKey)
        {
            using (var channel = _connection.CreateModel())
            {
                channel.QueueBind(queue: queueName, exchange: exchangeName, routingKey: routingKey);
            }
        }


        //Belirtilen kuyruğu siler.
        public void DeleteQueue(string queueName)
        {
            using (var channel = _connection.CreateModel())
            {
                channel.QueueDelete(queue: queueName);
            }
        }


        //Belirtilen kuyruktaki tüm mesajları temizler.
        public void PurgeQueue(string queueName)
        {
            using (var channel = _connection.CreateModel())
            {
                channel.QueuePurge(queue: queueName);
            }
        }



        //Bu metod, bir mesajın işlendiğini bildirerek ACK (acknowledgement) gönderir.
        //Mesajlar alındıktan sonra ACK gönderilmediği sürece RabbitMQ, bu mesajları hala kuyrukta tutar. deliveryTag parametresi,
        //mesajın benzersiz kimliğini temsil eder.
        public void AcknowledgeMessage(ulong deliveryTag)
        {
            _channel.BasicAck(deliveryTag: deliveryTag, multiple: false);
        }


        //Bu metod, bir mesajın işlenemediğini bildirerek NACK (negative acknowledgement) gönderir.
        //deliveryTag parametresi, reddedilen mesajın benzersiz kimliğini temsil eder. requeue parametresi,
        //true olarak ayarlandığında reddedilen mesajın tekrar kuyruğa eklenmesini sağlar, false olarak ayarlandığında ise mesajı kuyruktan siler.
        public void RejectMessage(ulong deliveryTag, bool requeue)
        {
            _channel.BasicReject(deliveryTag: deliveryTag, requeue: requeue);
        }


        //Bu metod, belirtilen kuyruktan mesaj tüketmeye başlar. queueName parametresi, tüketilecek kuyruğun adını belirtir.
        //messageHandler ise her bir mesaj için çağrılacak bir işlevdir. Bu işlev, tüketilen mesajın içeriğini alır ve istenilen işlemleri gerçekleştirir.
        public void StartConsuming(string queueName, Action<string> messageHandler)
        {
            var consumer = new EventingBasicConsumer(_channel);

            consumer.Received += (model, ea) =>
            {
                var body = ea.Body.ToArray();
                var message = Encoding.UTF8.GetString(body);

                messageHandler(message);

                _channel.BasicAck(deliveryTag: ea.DeliveryTag, multiple: false);
            };

            _consumerTag = _channel.BasicConsume(queue: queueName, autoAck: false, consumer: consumer);
        }


        //Bu metod, mesaj tüketmeyi durdurur. Başlatılan tüketim işlemini sonlandırır ve tüketiciyi kapatır.
        //Tüketim durduğunda, RabbitMQ'ya yeni mesaj tüketimi talebi gönderilmez.
        public void StopConsuming()
        {
            _channel.BasicCancel(_consumerTag);
        }






        public void Dispose()
        {
            _connection?.Dispose();
        }
    }
}
