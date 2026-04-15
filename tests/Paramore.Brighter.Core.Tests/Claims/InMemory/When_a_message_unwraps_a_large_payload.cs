using System;
using System.IO;
using Paramore.Brighter.Core.Tests.TestHelpers;
using Paramore.Brighter.Transforms.Storage;
using Paramore.Brighter.Transforms.Transformers;

namespace Paramore.Brighter.Core.Tests.Claims.InMemory;
public class RetrieveClaimLargePayloadTests
{
    private readonly InMemoryStorageProvider _store;
    private readonly ClaimCheckTransformer _transformerAsync;
    private readonly string _contents;
    public RetrieveClaimLargePayloadTests()
    {
        _store = new InMemoryStorageProvider();
        _transformerAsync = new ClaimCheckTransformer(_store, _store);
        //delete the luggage from the store after claiming it
        _transformerAsync.InitializeUnwrapFromAttributeParams(false);
        _contents = DataGenerator.CreateString(6000);
    }

    [Test]
    public async Task When_a_message_unwraps_a_large_payload()
    {
        //arrange
        var stream = new MemoryStream();
        var writer = new StreamWriter(stream);
        writer.Write(_contents);
        writer.Flush();
        stream.Position = 0;
        var id = _store.Store(stream);
        var message = new Message(new MessageHeader(Guid.NewGuid().ToString(), new RoutingKey("test_topic"), MessageType.MT_EVENT, timeStamp: DateTime.UtcNow), new MessageBody($"Claim Check {id}"));
        message.Header.DataRef = id;
        //act
        var unwrappedMessage = _transformerAsync.Unwrap(message);
        //assert
        await Assert.That(unwrappedMessage.Body.Value).IsEqualTo(_contents);
        //clean up
        await Assert.That(message.Header.DataRef).IsNull();
        await Assert.That(message.Header.Bag.TryGetValue(ClaimCheckTransformer.CLAIM_CHECK, out object _)).IsFalse();
        await Assert.That(_store.HasClaim(id)).IsFalse();
    }
}