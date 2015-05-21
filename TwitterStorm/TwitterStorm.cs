using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.SCP;
using Microsoft.SCP.Topology;
using TwitterStorm.Bolts;
using TwitterStorm.Spouts;

namespace TwitterStorm
{
    [Active(true)]
    public class TwitterStorm : TopologyDescriptor
    {
        public ITopologyBuilder GetTopologyBuilder()
        {
            TopologyBuilder topologyBuilder = new TopologyBuilder("TwitterStorm");

            topologyBuilder.SetSpout(
                typeof(TwitterSpout).Name,
                TwitterSpout.Get,
                new Dictionary<string, List<string>>()
                {
                    {Constants.DEFAULT_STREAM_ID, new List<string>(){"id", "tweet"}}
                },
                4,
                "TwitterStorm.config");

            topologyBuilder.SetBolt(
                typeof(BlobWriterBolt).Name,
                BlobWriterBolt.Get,
                new Dictionary<string, List<string>>()
                {
                    //{Constants.DEFAULT_STREAM_ID, new List<string>(){"id", "tweet"}}
                },
                4,
                "TwitterStorm.config").shuffleGrouping(typeof(TwitterSpout).Name);

            topologyBuilder.SetBolt(
                typeof(SentimentBolt).Name,
                SentimentBolt.Get,
                new Dictionary<string, List<string>>()
                {
                    {TwitterStormConstants.SENTIMENT_STREAM, new List<string>(){"id", "tweet", "sentiment"}}
                },
                8,
                "TwitterStorm.config").shuffleGrouping(typeof(TwitterSpout).Name);

            topologyBuilder.SetBolt(
               typeof(GeographyBolt).Name,
               GeographyBolt.Get,
               new Dictionary<string, List<string>>()
                {
                    {TwitterStormConstants.GEOGRAPHY_STREAM, new List<string>(){
                        "id",
                        "coordinates",
                        "admindistrict",
                        "admindistrict2",
                        "countryregion",
                        "locality",
                        "postalcode"
                    }}
                },
               8,
               "TwitterStorm.config")
               .shuffleGrouping(typeof(TwitterSpout).Name);

            topologyBuilder.SetBolt(
               typeof(MergeBolt).Name,
               MergeBolt.Get,
               new Dictionary<string, List<string>>()
                {
                    {Constants.DEFAULT_STREAM_ID, new List<string>(){
                        "id",
                        "coordinates",
                        "tweetjson",
                        "admindistrict",
                        "admindistrict2",
                        "countryregion",
                        "locality",
                        "postalcode",
                        "sentiment"
                    }}
                },
               1,
               "TwitterStorm.config")
               .fieldsGrouping(typeof(SentimentBolt).Name, TwitterStormConstants.SENTIMENT_STREAM, new List<int>() { 0 })
               .fieldsGrouping(typeof(GeographyBolt).Name, TwitterStormConstants.GEOGRAPHY_STREAM, new List<int>() { 0 });

            topologyBuilder.SetBolt(
                typeof(SwearWordBolt).Name,
                SwearWordBolt.Get,
                new Dictionary<string, List<string>>()
                {
                    {TwitterStormConstants.ARCHIVE_STREAM, new List<string>(){
                        "id",
                        "createddate",
                        "coordinates",
                        "tweetjson",
                        "admindistrict",
                        "admindistrict2",
                        "countryregion",
                        "locality",
                        "postalcode",
                        "sentiment",
                        "swearwordcount"
                    }},
                    {TwitterStormConstants.TOPIC_STREAM, new List<string>(){
                        "id",
                        "createddate",
                        "coordinates",
                        "topic",
                        "admindistrict",
                        "admindistrict2",
                        "countryregion",
                        "locality",
                        "postalcode",
                        "sentiment",
                        "swearwordcount"
                    }}
                },
                2,
                "TwitterStorm.config").shuffleGrouping(typeof(MergeBolt).Name);

            topologyBuilder.SetBolt(
                typeof(HBaseTweetBolt).Name,
                HBaseTweetBolt.Get,
                new Dictionary<string, List<string>>(),
                2,
                "TwitterStorm.config").shuffleGrouping(typeof(SwearWordBolt).Name, TwitterStormConstants.ARCHIVE_STREAM);

            topologyBuilder.SetBolt(
                typeof(HBaseTopicBolt).Name,
                HBaseTopicBolt.Get,
                new Dictionary<string, List<string>>(),
                2,
                "TwitterStorm.config").shuffleGrouping(typeof(SwearWordBolt).Name, TwitterStormConstants.TOPIC_STREAM);

            // Add topology config
            topologyBuilder.SetTopologyConfig(new Dictionary<string, string>()
            {
                {"topology.workers", "8"},
                {"topology.kryo.register","[\"[B\"]"}
            });

            return topologyBuilder;
        }
    }
}
