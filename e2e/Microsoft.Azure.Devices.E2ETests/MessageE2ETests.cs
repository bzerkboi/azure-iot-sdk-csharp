using System;
using System.Text;
using System.Threading.Tasks;

using Microsoft.VisualStudio.TestTools.UnitTesting;
using Microsoft.Azure.Devices;
using Microsoft.Azure.Devices.Client;
using Microsoft.Azure.Devices.Common;
using Microsoft.ServiceBus.Messaging;
using System.Text.RegularExpressions;

using System.Diagnostics;
using System.Linq;

namespace Microsoft.Azure.Devices.E2ETests
{
    [TestClass]
    public class MessageE2ETests
    {

        static string hubConnectionString;
        static string deviceName;
        static string deviceConnectionString;
        static string hostName;

        public TestContext TestContext { get; set; }

        static string GetHostName(string connectionString)
        {
            Regex regex = new Regex("HostName=([^;]+)", RegexOptions.None);
            return regex.Match(connectionString).Groups[1].Value;
        }

        static string GetDeviceConnectionString(Device device)
        {
            var connectionString = new StringBuilder();
            connectionString.AppendFormat("HostName={0}", hostName);
            connectionString.AppendFormat(";DeviceId={0}", device.Id);
            connectionString.AppendFormat(";SharedAccessKey={0}", device.Authentication.SymmetricKey.PrimaryKey);
            return connectionString.ToString();
        }

        [ClassInitialize]
        static public void ClassInitialize(TestContext testContext)
        {
            Task.Run(async () =>
            {
                hubConnectionString = Environment.GetEnvironmentVariable("IOTHUB_CONNECTION_STRING");
                deviceName = "E2E_Message_CSharp_" + Guid.NewGuid().ToString();
                deviceConnectionString = null;
                hostName = GetHostName(hubConnectionString);

                var registryManager = RegistryManager.CreateFromConnectionString(hubConnectionString);
                Debug.WriteLine("Creating device " + deviceName);
                var device = await registryManager.AddDeviceAsync(new Device(deviceName));
                deviceConnectionString = GetDeviceConnectionString(device);
                Debug.WriteLine("Device successfully created");
                await registryManager.CloseAsync();
            }).Wait();
        }

        [ClassCleanup]
        static public void ClassCleanup()
        {
            Task.Run(async () =>
            {
                var registryManager = RegistryManager.CreateFromConnectionString(hubConnectionString);

                Debug.WriteLine("Removing device " + deviceName);
                await registryManager.RemoveDeviceAsync(deviceName);
                Debug.WriteLine("Device successfully removed");
                await registryManager.CloseAsync();
            }).Wait();
        }

        [TestMethod]
        [TestCategory("Message-E2E")]
        public async Task Message_DeviceSendSingleMessage_Amqp()
        {
            await sendSingleMessage(Client.TransportType.Amqp_Tcp_Only);
        }

        [TestMethod]
        [TestCategory("Message-E2E")]
        public async Task Message_DeviceSendSingleMessage_AmqpWs()
        {
            await sendSingleMessage(Client.TransportType.Amqp_WebSocket_Only);
        }

        [TestMethod]
        [TestCategory("Message-E2E")]
        public async Task Message_DeviceSendSingleMessage_Mqtt()
        {
            await sendSingleMessage(Client.TransportType.Mqtt);
        }

        [TestMethod]
        [TestCategory("Message-E2E")]
        public async Task Message_DeviceSendSingleMessage_MqttWs()
        {
            await sendSingleMessage(Client.TransportType.Mqtt_WebSocket_Only);
        }

        [TestMethod]
        [TestCategory("Message-E2E")]
        public async Task Message_DeviceSendSingleMessage_Http()
        {
            await sendSingleMessage(Client.TransportType.Http1);
        }

        async Task sendSingleMessage(Client.TransportType transport)
        {
            EventHubClient eventHubClient = EventHubClient.CreateFromConnectionString(hubConnectionString, "messages/events");
            var eventHubPartitionsCount = eventHubClient.GetRuntimeInformation().PartitionCount;
            string partition = EventHubPartitionKeyResolver.ResolveToPartition(deviceName, eventHubPartitionsCount);
            string consumerGroupName = "$Default";
            EventHubReceiver eventHubReceiver = eventHubClient.GetConsumerGroup(consumerGroupName).CreateReceiver(partition, DateTime.Now);

            var deviceClient = DeviceClient.CreateFromConnectionString(deviceConnectionString, transport);
            await deviceClient.OpenAsync();

            string dataBuffer = Guid.NewGuid().ToString();
            string propertyName = "property1";
            string propertyValue = Guid.NewGuid().ToString();
            Client.Message eventMessage = new Client.Message(Encoding.UTF8.GetBytes(dataBuffer));
            eventMessage.Properties[propertyName] = propertyValue;
            await deviceClient.SendEventAsync(eventMessage);

            var events = await eventHubReceiver.ReceiveAsync(int.MaxValue, TimeSpan.FromSeconds(20));

            foreach (var eventData in events)
            {
                var data = Encoding.UTF8.GetString(eventData.GetBytes());
                Assert.AreEqual(data, dataBuffer);

                var connectionDeviceId = eventData.SystemProperties["iothub-connection-device-id"].ToString();
                Assert.AreEqual(connectionDeviceId.ToUpper(), deviceName.ToUpper());

                Assert.AreEqual(eventData.Properties.Count, 1);
                var property = eventData.Properties.Single();
                Assert.AreEqual(property.Key, propertyName);
                Assert.AreEqual(property.Value, propertyValue);
            }

            await deviceClient.CloseAsync();
            await eventHubClient.CloseAsync();
        }

        [TestMethod]
        [TestCategory("Message-E2E")]
        public async Task Message_DeviceReceiveSingleMessage_Amqp()
        {
            await receiveSingleMessage(Client.TransportType.Amqp_Tcp_Only);
        }

        [TestMethod]
        [TestCategory("Message-E2E")]
        public async Task Message_DeviceReceiveSingleMessage_AmqpWs()
        {
            await receiveSingleMessage(Client.TransportType.Amqp_WebSocket_Only);
        }

        [TestMethod]
        [TestCategory("Message-E2E")]
        public async Task Message_DeviceReceiveSingleMessage_Mqtt()
        {
            await receiveSingleMessage(Client.TransportType.Mqtt);
        }

        [TestMethod]
        [TestCategory("Message-E2E")]
        public async Task Message_DeviceReceiveSingleMessage_MqttWs()
        {
            await receiveSingleMessage(Client.TransportType.Mqtt_WebSocket_Only);
        }

        [TestMethod]
        [TestCategory("Message-E2E")]
        public async Task Message_DeviceReceiveSingleMessage_Http()
        {
            await receiveSingleMessage(Client.TransportType.Http1);
        }

        async Task receiveSingleMessage(Client.TransportType transport)
        {
            ServiceClient serviceClient = ServiceClient.CreateFromConnectionString(hubConnectionString);
            string dataBuffer = Guid.NewGuid().ToString();
            var serviceMessage = new Message(Encoding.ASCII.GetBytes(dataBuffer));
            serviceMessage.MessageId = Guid.NewGuid().ToString();

            string propertyName = "property1";
            string propertyValue = Guid.NewGuid().ToString();
            serviceMessage.Properties[propertyName] = propertyValue;

            var deviceClient = DeviceClient.CreateFromConnectionString(deviceConnectionString, transport);
            await deviceClient.OpenAsync();
            if (transport == Client.TransportType.Mqtt_Tcp_Only || transport == Client.TransportType.Mqtt_WebSocket_Only)
            {
                // Dummy ReceiveAsync to ensure mqtt subscription registration before SendAsync() is called on service client.
                await deviceClient.ReceiveAsync(TimeSpan.FromSeconds(2));
            }

            await serviceClient.OpenAsync();
            await serviceClient.SendAsync(deviceName, serviceMessage);

            var wait = true;
            while (wait)
            {
                Client.Message receivedMessage;

                if (transport == Client.TransportType.Http1)
                {
                    // Long-polling is not supported in http
                    receivedMessage = await deviceClient.ReceiveAsync();
                }
                else
                {
                    receivedMessage = await deviceClient.ReceiveAsync(TimeSpan.FromSeconds(1));
                }

                if (receivedMessage != null)
                {
                    string messageData = Encoding.ASCII.GetString(receivedMessage.GetBytes());
                    Assert.AreEqual(messageData, dataBuffer);

                    Assert.AreEqual(receivedMessage.Properties.Count, 1);
                    var prop = receivedMessage.Properties.Single();
                    Assert.AreEqual(prop.Key, propertyName);
                    Assert.AreEqual(prop.Value, propertyValue);

                    await deviceClient.CompleteAsync(receivedMessage);
                    wait = false;
                }
            }

            await deviceClient.CloseAsync();
            await serviceClient.CloseAsync();
        }
    }
}

