using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using EventStore.ClientAPI;
using EventStore.ClientAPI.ClientOperations;
using EventStore.ClientAPI.Common.Log;
using EventStore.ClientAPI.Internal;
using NUnit.Framework;
using ClientMessage = EventStore.ClientAPI.Messages.ClientMessage;
using ResolvedEvent = EventStore.ClientAPI.ResolvedEvent;
using StreamMetadata = EventStore.ClientAPI.StreamMetadata;
using SystemSettings = EventStore.ClientAPI.SystemSettings;

namespace EventStore.Core.Tests.ClientAPI
{
    [TestFixture, Category("ClientAPI"), Category("LongRunning")]
    public class catch_up_subscription_to_all_handles_non_replicated_events
    {
        private static readonly int TimeoutMs = 2000;
        private static readonly string StreamId = "catch_up_subscription_to_all_handles_non_replicated_events";
        private const int _expectedReads = 5;
        private const int _lastEventPosition = 2000;
        private int _readCount;

        private FakeEventStoreConnection _connection = new FakeEventStoreConnection();
        private IList<ResolvedEvent> _raisedEvents = new List<ResolvedEvent>();
        private ManualResetEventSlim _liveProcessingEvent = new ManualResetEventSlim();

        private bool _liveProcessingStarted;
        private EventStoreAllCatchUpSubscription _subscription;

        [OneTimeSetUp]
        public void SetUp()
        {
            var settings = new CatchUpSubscriptionSettings(5, 5, false, false, String.Empty);
            _subscription = new EventStoreAllCatchUpSubscription(_connection, new ConsoleLogger(), null, null,
                (subscription, ev) =>
                {
                    _raisedEvents.Add(ev);
                    return Task.CompletedTask;
                },
                subscription =>
                {
                    _liveProcessingStarted = true;
                    _liveProcessingEvent.Set();
                },
                (subscription, reason, ex) =>
                {
                },
                settings);

            // When there are only non-replicated events left in a read, the read will return EOF.
            // When the subscription tries to go live, it will read up to the last event number returned by the subscription confirmation
            // Even if the read returns EOF, the subscription will read in a loop until the event number catches up.
            _connection.HandleReadAllEventsForwardAsync((start, count, resolveLinkTos, credentials) =>
            {
                var taskCompletionSource = new TaskCompletionSource<AllEventsSlice>(TaskCreationOptions.RunContinuationsAsynchronously);

                Position nextPosition = start;
                var events = new List<ClientMessage.ResolvedEvent>();

                _readCount++;
                if(start.CommitPosition == 0)
                {
                    events.Add(CreateEvent(0, start.CommitPosition));
                    nextPosition = new Position(start.CommitPosition + 1000, start.PreparePosition + 1000);
                }
                else if (_readCount == _expectedReads)
                {
                    events.Add(CreateEvent(1, start.CommitPosition));
                    nextPosition = new Position(start.CommitPosition + 1000, start.PreparePosition + 1000);
                }
                var read = new AllEventsSlice(ReadDirection.Forward, start, nextPosition, events.ToArray());
                _readCount++;
                taskCompletionSource.SetResult(read);
                return taskCompletionSource.Task;
            });

            _connection.HandleSubscribeToAllAsync((stream, raise, drop) =>
            {
                var taskCompletionSource = new TaskCompletionSource<EventStoreSubscription>(TaskCreationOptions.RunContinuationsAsynchronously);
                var sub = new VolatileEventStoreSubscription(
                    new VolatileSubscriptionOperation(new NoopLogger(),
                        new TaskCompletionSource<EventStoreSubscription>(TaskCreationOptions.RunContinuationsAsynchronously),
                        null, false, null, raise, drop, false, () => null), null, _lastEventPosition, -1);
                taskCompletionSource.SetResult(sub);
                return taskCompletionSource.Task;
            });

            _subscription.StartAsync().Wait(TimeoutMs);

            if(!_liveProcessingEvent.Wait(TimeSpan.FromSeconds(TimeoutMs))) {
                Assert.Fail("Timed out waiting for subscription to go live");
            }
        }

        [OneTimeTearDown]
        public void TearDown()
        {
            _subscription.DropSubscription(SubscriptionDropReason.UserInitiated, null);
        }

        private ClientMessage.ResolvedEvent CreateEvent(long eventNumber, long logPosition)
        {
            var evnt = new ClientMessage.EventRecord(StreamId, eventNumber, Guid.NewGuid().ToByteArray(), "eventType", 1, 1, new byte[10], new byte[0], null, null);
            return new ClientMessage.ResolvedEvent(evnt, evnt, logPosition, logPosition);
        }

        [Test]
        public void subscription_successfully_goes_live()
        {
            Assert.IsTrue(_liveProcessingStarted);
        }

        [Test]
        public void subscription_receives_the_events()
        {
            Assert.AreEqual(2, _raisedEvents.Count, "Record count");
            Assert.AreEqual(0, _raisedEvents[0].OriginalEventNumber, "Event number 0");
            Assert.AreEqual(1, _raisedEvents[1].OriginalEventNumber, "Event number 1");
        }
    }
}