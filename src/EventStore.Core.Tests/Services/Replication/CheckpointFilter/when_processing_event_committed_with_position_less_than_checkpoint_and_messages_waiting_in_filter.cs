using System.Linq;
using EventStore.Core.Messages;
using NUnit.Framework;

namespace EventStore.Core.Tests.Services.Replication.CheckpointFilter
{
    [TestFixture]
    public class when_processing_event_committed_with_position_less_than_checkpoint_and_messages_waiting_in_filter : with_replication_checkpoint_filter
    {
        public override void When()
        {
            _replicationChk.Write(0);

            var msg = new StorageMessage.EventCommitted(1000, CreateDummyEventRecord(1000), false);
            _filter.Handle(msg);

            msg = new StorageMessage.EventCommitted(2000, CreateDummyEventRecord(2000), false);
            _filter.Handle(msg);

            _replicationChk.Write(3000);
            msg = new StorageMessage.EventCommitted(3000, CreateDummyEventRecord(3000), false);
            _filter.Handle(msg);
        }

        [Test]
        public void should_publish_all_event_committed_messages()
        {
            Assert.AreEqual(3, _outputConsumer.HandledMessages.OfType<StorageMessage.EventCommitted>().Count());
        }
    }
}