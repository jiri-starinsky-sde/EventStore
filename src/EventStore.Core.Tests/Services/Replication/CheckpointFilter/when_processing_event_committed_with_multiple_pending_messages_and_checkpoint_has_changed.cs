using System.Linq;
using EventStore.Core.Messages;
using NUnit.Framework;

namespace EventStore.Core.Tests.Services.Replication.CheckpointFilter
{
    [TestFixture]
    public class when_processing_event_committed_with_multiple_pending_messages_and_checkpoint_has_changed : with_replication_checkpoint_filter
    {
        public override void When()
        {
            _replicationChk.Write(-1);

            var msg1 = new StorageMessage.EventCommitted(1000, CreateDummyEventRecord(1000), false);
            var msg2 = new StorageMessage.EventCommitted(2000, CreateDummyEventRecord(2000), false);
            _filter.Handle(msg1);
            _filter.Handle(msg2);
            _replicationChk.Write(2000);

            var msg3 = new StorageMessage.EventCommitted(3000, CreateDummyEventRecord(3000), false);
            _filter.Handle(msg3);
        }

        [Test]
        public void should_publish_event_committed_message_for_first_two_messages()
        {
            var messages = _outputConsumer.HandledMessages.OfType<StorageMessage.EventCommitted>().ToList();
            Assert.AreEqual(2, messages.Count);
            Assert.IsTrue(messages.Any(x=> ((StorageMessage.EventCommitted)x).CommitPosition == 1000));
            Assert.IsTrue(messages.Any(x=> ((StorageMessage.EventCommitted)x).CommitPosition == 2000));
        }

        [Test]
        public void should_not_publish_third_message()
        {
            var message = _outputConsumer.HandledMessages.OfType<StorageMessage.EventCommitted>().FirstOrDefault(x => x.CommitPosition == 3000);
            Assert.IsNull(message);
        }
    }
}