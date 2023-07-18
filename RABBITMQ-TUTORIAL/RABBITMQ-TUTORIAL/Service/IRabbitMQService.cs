using RabbitMQ.Client;

namespace RABBITMQ_TUTORIAL.Service
{
    public interface IRabbitMQService
    {
        void PublishMessage(string queueName, string message);

        void ProduceSpecialMessageWithUserId(int userId, string queueName, string message);

        void ConsomeSpecialMessageWithUserId(int user_id, string queueName);

        List<string> GetMessagesFromQueue(string queueName);

        int GetMessageCount(IModel channel, string queueName);

        void CreateExchange(string exchangeName, string exchangeType);
        void BindQueue(string queueName, string exchangeName, string routingKey);
        void DeleteQueue(string queueName);
        void PurgeQueue(string queueName);
        void AcknowledgeMessage(ulong deliveryTag);
        void RejectMessage(ulong deliveryTag, bool requeue);
        void StartConsuming(string queueName, Action<string> messageHandler);
        void StopConsuming();
    }
}
