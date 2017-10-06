using System;
using System.Collections.Generic;
using System.Linq;
using EventStore.Common.Log;
using EventStore.Core.Bus;
using EventStore.Core.Messages;
using EventStore.Core.Messaging;
using EventStore.Core.Services.TimerService;
using EventStore.Core.TransactionLog.Checkpoint;

namespace EventStore.Core.Services.Replication
{
    public class ReplicationCheckpointFilter : IHandle<ReplicationMessage.ReplicationCheckTick>
    {
        public ILogger Log = LogManager.GetLoggerFor<ReplicationCheckpointFilter>();
        private readonly IPublisher _outputBus;
        private readonly IPublisher _publisher;
        private readonly IEnvelope _busEnvelope;
        private readonly ICheckpoint _replicationCheckpoint;
        private readonly TimeSpan TimeoutPeriod = new TimeSpan(100);

        private long _lastReplicationCheckpoint;
        private Dictionary<long, List<Message>> _messages;

        public ReplicationCheckpointFilter(IPublisher outputBus, IPublisher publisher, ICheckpoint replicationCheckpoint)
        {
            _outputBus = outputBus;
            _publisher = publisher;
            _replicationCheckpoint = replicationCheckpoint;
            _busEnvelope = new PublishEnvelope(_publisher);
            _messages = new Dictionary<long, List<Message>>();
            _publisher.Publish(TimerMessage.Schedule.Create(TimeoutPeriod, _busEnvelope, new ReplicationMessage.ReplicationCheckTick()));
        }

        public void Handle(StorageMessage.EventCommitted message)
        {
            Enqueue(message, message.CommitPosition);
            HandleMessages();
        }

        public void Handle(ReplicationMessage.ReplicationCheckTick message)
        {
            HandleMessages();
            _publisher.Publish(TimerMessage.Schedule.Create(TimeoutPeriod, _busEnvelope, message));
        }

        private void Enqueue(Message message, long commitPosition)
        {
            if(_messages.ContainsKey(commitPosition))
            {
                _messages[commitPosition].Add(message);
            }
            else
            {
                _messages.Add(commitPosition, new List<Message>{ message });
            }
        }

        private void HandleMessages()
        {
            var replChk = _replicationCheckpoint.ReadNonFlushed();
            if(replChk == _lastReplicationCheckpoint) return;

            _lastReplicationCheckpoint = replChk;
            var messagesToHandle = _messages.Where(x => x.Key <= replChk).ToList();
            foreach(var m in messagesToHandle)
            {
                foreach(var i in m.Value)
                {
                    _outputBus.Publish(i);
                }
                _messages.Remove(m.Key);
            }
        }
    }
}