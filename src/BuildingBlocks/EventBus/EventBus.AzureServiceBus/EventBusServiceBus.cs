using System.Text;
using EventBus.Base;
using EventBus.Base.Events;
using Microsoft.Azure.ServiceBus;
using Microsoft.Azure.ServiceBus.Management;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;

namespace EventBus.AzureServiceBus;

public class EventBusServiceBus : BaseEventBus
{
    private ITopicClient _topicClient;
    private ManagementClient _managementClient;
    private readonly ILogger? _logger;

    public EventBusServiceBus(EventBusConfig config, IServiceProvider serviceProvider) : base(config, serviceProvider)
    {
        _logger = serviceProvider.GetService(typeof(ILogger<EventBusServiceBus>)) as ILogger<EventBusServiceBus>;
        _managementClient = new ManagementClient(config.EventBusConnectionString);
        _topicClient = CreateTopicClient();

    }

    private ITopicClient CreateTopicClient()
    {
        if (_topicClient == null || _topicClient.IsClosedOrClosing)
        {
            _topicClient = new TopicClient(EventBusConfig.EventBusConnectionString, EventBusConfig.DefaultTopicName, RetryPolicy.Default);
        }

        if (!_managementClient.TopicExistsAsync(EventBusConfig.DefaultTopicName).GetAwaiter().GetResult())
            _managementClient.CreateTopicAsync(EventBusConfig.DefaultTopicName).GetAwaiter().GetResult();

        return _topicClient;
    }

    public override void Publish(IntegrationEvent @event)
    {
        var eventName = @event.GetType().Name;

        eventName = ProcessEventName(eventName);

        var eventStr = JsonConvert.SerializeObject(@eventName);
        var bodyArr = Encoding.UTF8.GetBytes(eventStr);

        var message = new Message()
        {
            MessageId = Guid.NewGuid().ToString(),
            Body = bodyArr,
            Label = eventName
        };

        _topicClient.SendAsync(message);
    }

    public override void Subscribe<T, THandler>()
    {
        var eventName = typeof(T).Name;
        eventName = ProcessEventName(eventName);


        if (!SubsManager.HasSubscriptionsForEvent(eventName))
        {
            var subscriptionClient = CreateSubscriptionClientIfNotExists(eventName);

            RegisterSubscriptionClientMessageHandler(subscriptionClient);
        }

        _logger?.LogInformation("Subscribing to event {EventName} with {EventHandler}", eventName, typeof(THandler).Name);

        SubsManager.AddSubscription<T, THandler>();
    }

    public override void UnSubscribe<T, THandler>()
    {
        var eventName = typeof(T).Name;

        try
        {
            var subscriptionClient = CreateSubscriptionClient(eventName);
            subscriptionClient.RemoveRuleAsync(eventName)
                .GetAwaiter().GetResult();
        }
        catch (MessagingEntityNotFoundException)
        {
            _logger?.LogWarning("The messaging entity {eventName} Could not be found.", eventName);
        }

        _logger?.LogInformation("Unsubscribing from event {eventName}", eventName);

        SubsManager.RemoveSubscription<T, THandler>();
    }

    public override void Dispose()
    {
        base.Dispose();

        _topicClient.CloseAsync().GetAwaiter().GetResult();
        _managementClient.CloseAsync().GetAwaiter().GetResult();
        _topicClient = null;
        _managementClient = null;
    }

    private void RegisterSubscriptionClientMessageHandler(ISubscriptionClient subscriptionClient)
    {
        subscriptionClient.RegisterMessageHandler(
            async (message, token) =>
            {
                var eventName = message.Label;
                var messageData = Encoding.UTF8.GetString(message.Body);

                //Complete the message so that it is not received again
                if (await ProcessEvent(ProcessEventName(eventName), messageData))
                {
                    await subscriptionClient.CompleteAsync(message.SystemProperties.LockToken);
                }
            },
            new MessageHandlerOptions(ExceptionReceiveHandler)
            {
                MaxConcurrentCalls = 10,
                AutoComplete = false
            });
    }

    private Task ExceptionReceiveHandler(ExceptionReceivedEventArgs exceptionReceivedEventArgs)
    {
        var ex = exceptionReceivedEventArgs.Exception;
        var context = exceptionReceivedEventArgs.ExceptionReceivedContext;

        _logger?.LogError(ex, "ERROR handling message: {ExceptionMessage} - Context: {@ExceptionContext}", ex.Message, context);
        return Task.CompletedTask;
    }

    private SubscriptionClient CreateSubscriptionClient(string eventName)
    {
        return new SubscriptionClient(EventBusConfig.EventBusConnectionString, EventBusConfig.DefaultTopicName, GetSubName(eventName));
    }

    private ISubscriptionClient CreateSubscriptionClientIfNotExists(string eventName)
    {
        var subClient = CreateSubscriptionClient(eventName);

        var exists = _managementClient.SubscriptionExistsAsync(EventBusConfig.DefaultTopicName, GetSubName(eventName)).GetAwaiter().GetResult();

        if (!exists)
        {
            _managementClient.CreateSubscriptionAsync(EventBusConfig.DefaultTopicName, GetSubName(eventName)).GetAwaiter().GetResult();
            RemoveDefaultRule(subClient);
        }

        CreateRuleIfNotExists(ProcessEventName(eventName), subClient);

        return subClient;
    }

    private void CreateRuleIfNotExists(string eventName, ISubscriptionClient subscriptionClient)
    {
        bool ruleExists;

        try
        {
            var rule = _managementClient.GetRuleAsync(EventBusConfig.DefaultTopicName, eventName, eventName).GetAwaiter().GetResult();
            ruleExists = rule != null;
        }
        catch (MessagingEntityNotFoundException)
        {
            ruleExists = false;
        }

        if (!ruleExists)
        {
            subscriptionClient.AddRuleAsync(new RuleDescription()
            {
                Name = eventName,
                Filter = new CorrelationFilter { Label = eventName }
            }).GetAwaiter().GetResult();
        }
    }

    private void RemoveDefaultRule(SubscriptionClient subscriptionClient)
    {
        try
        {
            subscriptionClient.RemoveRuleAsync(RuleDescription.DefaultRuleName)
                .GetAwaiter().GetResult();
        }
        catch (MessagingEntityNotFoundException)
        {
            _logger?.LogWarning("The messaging entity {DefaultRuleName} Could not be found.", RuleDescription.DefaultRuleName);
        }
    }

}