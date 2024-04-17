using MQTTnet;
using MQTTnet.Client;
using MQTTnet.Formatter;
using MQTTnet.Protocol;
using MQTTnet.Server;
namespace repro.test;

public class UnitTest1 : IDisposable
{
    private readonly MqttServer mqttServer;
    private readonly IMqttClient mqttClient;
    private readonly MqttClientSubscribeOptions subscriptionOptions;
    private readonly MqttClientOptions options = new MqttClientOptionsBuilder()
            .WithProtocolVersion(MqttProtocolVersion.V500)
            .WithTcpServer("localhost", 1883)
            .WithWillRetain()
            .Build();
    private readonly string topic = "testTopic";


    public UnitTest1()
    {
        var mqttFactory = new MqttFactory();
        var serverOptions = mqttFactory.CreateServerOptionsBuilder().WithDefaultEndpoint().WithDefaultEndpointPort(1883).Build();
        mqttServer = mqttFactory.CreateMqttServer(serverOptions);

        mqttClient = mqttFactory.CreateMqttClient();
        subscriptionOptions = mqttFactory
            .CreateSubscribeOptionsBuilder()
            .WithTopicFilter(f => f.WithTopic(topic))
            .Build();
    }

    private async Task InitAsync()
    {
        await mqttServer.StartAsync();
        while (!mqttServer.IsStarted)
        {
            await Task.Delay(100);
        }
        await mqttClient.ConnectAsync(options);
    }

    [Fact]
    public async Task Test_ReceiveMessageWhileSubscribing_Workaround_WillSucceed()
    {
        await InitAsync();
        mqttServer.InterceptingSubscriptionAsync += OnInterceptingSubscriptionWorkaroundAsync; // when this handler is used it waits a bit before sending the first message, the test will succeed

        await ActAndAssert();
    }

    [Fact]
    public async Task Test_ReceiveMessageWhileSubscribing_WillFail()
    {
        await InitAsync();
        mqttServer.InterceptingSubscriptionAsync += OnInterceptingSubscriptionAsync; // when this handler is used, the test will fail

        await ActAndAssert();
    }

    private async Task ActAndAssert()
    {
        // Act
        // write payload of message into TaskCompletionSource when a message happens, to be able to await the message
        var tcs = new TaskCompletionSource<string>();
        mqttClient.ApplicationMessageReceivedAsync += (e) =>
        {
            tcs.SetResult(e.ApplicationMessage.ConvertPayloadToString());
            return Task.CompletedTask;
        };

        // Subscribe to topic
        var result = await mqttClient.SubscribeAsync(subscriptionOptions);
        if (result.Items.Any((it) => it.ResultCode > MqttClientSubscribeResultCode.GrantedQoS2))
        {
            throw new Exception($"could not subscribe to topic {topic}");
        }

        // Assert
        try
        {
            // wait for the message to be received
            var payload = await tcs.Task.WaitAsync(new CancellationTokenSource(TimeSpan.FromSeconds(20)).Token);
            Assert.Equal("test", payload);
        }
        catch (OperationCanceledException ex)
        {
            // message was not received within cancellation time frame
            Assert.Fail(ex.ToString());
        }
    }

    private Task OnInterceptingSubscriptionWorkaroundAsync(InterceptingSubscriptionEventArgs args)
    {
        _ = Task.Run(async () =>
        {
            await Task.Delay(1000); // when waiting a bit before sending the initial message it works
            var message = new MqttApplicationMessageBuilder()
                .WithTopic(args.TopicFilter.Topic)
                .WithContentType("application/json")
                .WithPayloadFormatIndicator(MqttPayloadFormatIndicator.CharacterData)
                .WithPayload("test")
                .WithQualityOfServiceLevel(MqttQualityOfServiceLevel.ExactlyOnce)
                .WithRetainFlag()
                .Build();
            await mqttServer.InjectApplicationMessage(new InjectedMqttApplicationMessage(message), default);
        });
        return Task.CompletedTask;
    }

    private async Task OnInterceptingSubscriptionAsync(InterceptingSubscriptionEventArgs args)
    {

        var message = new MqttApplicationMessageBuilder()
            .WithTopic(args.TopicFilter.Topic)
            .WithContentType("application/json")
            .WithPayloadFormatIndicator(MqttPayloadFormatIndicator.CharacterData)
            .WithPayload("test")
            .WithQualityOfServiceLevel(MqttQualityOfServiceLevel.ExactlyOnce)
            .WithRetainFlag()
            .Build();
        await mqttServer.InjectApplicationMessage(new InjectedMqttApplicationMessage(message), default);
    }

    public void Dispose()
    {
        mqttClient.DisconnectAsync().Wait();
        mqttServer.StopAsync().Wait();
    }
}