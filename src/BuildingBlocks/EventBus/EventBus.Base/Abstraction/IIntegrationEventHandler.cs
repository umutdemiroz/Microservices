namespace EventBus.Base.Abstraction;

public interface IIntegrationEventHandler<TIntegrationEvent> : IntegrationEventHandler where TIntegrationEvent : IntegrationEventHandler
{
    Task Handle(TIntegrationEvent @event);
}

public interface IntegrationEventHandler { }