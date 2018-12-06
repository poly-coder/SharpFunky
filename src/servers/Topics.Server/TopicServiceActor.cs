using Akka.Actor;
using Akka.Event;
using Akka.Pattern;
using Grpc.Core;
using Microsoft.Extensions.Configuration;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using Topics.Protocols;

namespace Topics.Server
{
    public class TopicServiceOptions
    {
        public string Host { get; set; }
        public int Port { get; set; }
    }

    public class TopicServiceActor : UntypedActor
    {
        private Grpc.Core.Server server;
        private TopicServiceOptions options;
        private ILoggingAdapter log;

        public TopicServiceActor(TopicServiceOptions options)
        {
            this.options = options;
            this.log = Logging.GetLogger(Context);
        }

        protected override void PreStart()
        {
            server = new Grpc.Core.Server()
            {
                Ports =
                {
                    new ServerPort(options.Host, options.Port, ServerCredentials.Insecure),
                },
                Services =
                {
                    TopicService.BindService(new TopicServiceImpl())
                }
            };
            log.Info("Topic Service listening on {0}:{1}", options.Host, options.Port);
            server.Start();
        }

        protected override void PostStop()
        {
            ShutdownServer();
        }

        private void ShutdownServer()
        {
            if (server != null)
            {
                log.Info("Topic Service shutting down ...");
                server.ShutdownAsync();
                server = null;
            }
        }

        protected override void OnReceive(object message)
        {
            switch (message)
            {
                default:
                    break;
            }
        }
    }

    public class TopicServiceImpl: TopicService.TopicServiceBase
    {
        public async override Task<PublishedProtoMessage> PublishMessage(PublishProtoMessage request, ServerCallContext context)
        {
            return new PublishedProtoMessage
            {
                MessageId = request.MessageId,
                Sequence = 1,
                Timestamp = DateTime.UtcNow.Ticks,
                IsError = false
            };
        }
    }
}
