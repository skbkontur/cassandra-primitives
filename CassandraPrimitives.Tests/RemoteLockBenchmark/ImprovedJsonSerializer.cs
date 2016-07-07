using System;
using System.Net;

using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark
{
    public class ImprovedJsonSerializer
    {
        private static readonly JsonSerializerSettings settings;

        private class IpAddressConverter : JsonConverter
        {
            public override bool CanConvert(Type objectType)
            {
                return (objectType == typeof(IPAddress));
            }

            public override void WriteJson(JsonWriter writer, object value, JsonSerializer serializer)
            {
                var ip = (IPAddress)value;
                writer.WriteValue(ip.ToString());
            }

            public override object ReadJson(JsonReader reader, Type objectType, object existingValue, JsonSerializer serializer)
            {
                var token = JToken.Load(reader);
                return IPAddress.Parse(token.Value<string>());
            }
        }

        private class IpEndPointConverter : JsonConverter
        {
            public override bool CanConvert(Type objectType)
            {
                return (objectType == typeof(IPEndPoint));
            }

            public override void WriteJson(JsonWriter writer, object value, JsonSerializer serializer)
            {
                var endPoint = (IPEndPoint)value;
                writer.WriteStartObject();
                writer.WritePropertyName("Address");
                serializer.Serialize(writer, endPoint.Address);
                writer.WritePropertyName("Port");
                writer.WriteValue(endPoint.Port);
                writer.WriteEndObject();
            }

            public override object ReadJson(JsonReader reader, Type objectType, object existingValue, JsonSerializer serializer)
            {
                var jo = JObject.Load(reader);
                var address = jo["Address"].ToObject<IPAddress>(serializer);
                var port = jo["Port"].Value<int>();
                return new IPEndPoint(address, port);
            }
        }

        static ImprovedJsonSerializer()
        {
            settings = new JsonSerializerSettings();
            settings.Converters.Add(new IpAddressConverter());
            settings.Converters.Add(new IpEndPointConverter());
            settings.Formatting = Formatting.Indented;
        }

        public static string Serialize<T>(T item)
        {
            return JsonConvert.SerializeObject(item, settings);
        }

        public static T Deserialize<T>(string data)
        {
            return JsonConvert.DeserializeObject<T>(data, settings);
        }
    }
}