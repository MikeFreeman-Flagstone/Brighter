using System;
using System.Linq;
using Paramore.Brighter.Core.Tests.CommandProcessors.TestDoubles;

namespace Paramore.Brighter.Core.Tests.CommandProcessors.Pipeline
{
    public class PipelineCleanupTests
    {
        private readonly PipelineBuilder<MyCommand> _pipelineBuilder;
        private static string s_released;
        public PipelineCleanupTests()
        {
            s_released = string.Empty;
            var registry = new SubscriberRegistry();
            registry.Register<MyCommand, MyPreAndPostDecoratedHandler>();
            registry.Register<MyCommand, MyLoggingHandler<MyCommand>>();
            var handlerFactory = new CheapHandlerFactorySync();
            _pipelineBuilder = new PipelineBuilder<MyCommand>(registry, handlerFactory);
            _pipelineBuilder.Build(new MyCommand(), new RequestContext()).Any();
        }

        [Test]
        public async Task When_We_Have_Exercised_The_Pipeline_Cleanup_Its_Handlers()
        {
            _pipelineBuilder.Dispose();
            await Assert.That(MyPreAndPostDecoratedHandler.DisposeWasCalled).IsTrue();
            await Assert.That(MyLoggingHandler<MyCommand>.DisposeWasCalled).IsTrue();
            await Assert.That(s_released).IsEqualTo("|MyValidationHandler`1|MyPreAndPostDecoratedHandler|MyLoggingHandler`1|MyLoggingHandler`1");
        }

        internal sealed class CheapHandlerFactorySync : Paramore.Brighter.IAmAHandlerFactorySync, Paramore.Brighter.IAmAHandlerFactory
        {
            public IHandleRequests Create(Type handlerType, IAmALifetime lifetime)
            {
                if (handlerType == typeof(MyPreAndPostDecoratedHandler))
                {
                    return new MyPreAndPostDecoratedHandler();
                }

                if (handlerType == typeof(MyLoggingHandler<MyCommand>))
                {
                    return new MyLoggingHandler<MyCommand>();
                }

                if (handlerType == typeof(MyValidationHandler<MyCommand>))
                {
                    return new MyValidationHandler<MyCommand>();
                }

                return null;
            }

            public void Release(IHandleRequests handler, IAmALifetime lifetime)
            {
                var disposable = handler as IDisposable;
                disposable?.Dispose();
                s_released += "|" + handler.Name;
            }
        }
    }
}