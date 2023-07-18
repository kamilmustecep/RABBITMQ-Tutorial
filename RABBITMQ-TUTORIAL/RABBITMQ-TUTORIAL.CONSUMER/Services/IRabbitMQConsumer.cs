using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace RABBITMQ_TUTORIAL.CONSUMER.Services
{
    public interface IRabbitMQConsumer
    {
        void StartConsuming(string queueName);
        void StopConsuming();
        void ConsomeSpecialMessageWithUserId(int user_id, string queueName);
    }
}
