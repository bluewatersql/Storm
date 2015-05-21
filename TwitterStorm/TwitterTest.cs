using System;
using System.Configuration;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;
using System.Threading;
using Microsoft.SCP;
using TwitterStorm.Bolts;
using TwitterStorm.Spouts;

namespace TwitterStorm
{
    class TwitterTest
    {
        public void RunTestCase()
        {
            Dictionary<string, Object> emptyDictionary = new Dictionary<string, object>();

            ExeConfigurationFileMap configMap = new ExeConfigurationFileMap();
            configMap.ExeConfigFilename = @"TwitterStorm.config";

            Configuration config = ConfigurationManager.OpenMappedExeConfiguration(configMap, ConfigurationUserLevel.None);

            emptyDictionary.Add("UserConfig", config);

            Console.WriteLine("Starting...");
            {
                LocalContext ctx = LocalContext.Get();
                TwitterSpout spout = TwitterSpout.Get(ctx, emptyDictionary);

                for (int i = 0; i < 5; i++)
                {
                    spout.NextTuple(emptyDictionary);
                }

                ctx.WriteMsgQueueToFile("twitterspout.txt");
            }
            Console.WriteLine("Twitter spout done...");

            {
                LocalContext ctx = LocalContext.Get();
                BlobWriterBolt bolt = BlobWriterBolt.Get(ctx, emptyDictionary);

                ctx.ReadFromFileToMsgQueue("twitterspout.txt");

                List<SCPTuple> batch = ctx.RecvFromMsgQueue();

                int i = 0;

                foreach (SCPTuple tuple in batch)
                {
                    bolt.Execute(tuple);

                    i++;

                    if (i > 10)
                        break;
                }
                ctx.WriteMsgQueueToFile("blobwriterbolt.txt");
            }
            Console.WriteLine("Blob writer done...");

            {
                LocalContext ctx = LocalContext.Get();
                SentimentBolt bolt = SentimentBolt.Get(ctx, emptyDictionary);

                ctx.ReadFromFileToMsgQueue("twitterspout.txt");
                List<SCPTuple> batch = ctx.RecvFromMsgQueue();
                foreach (SCPTuple tuple in batch)
                {
                    bolt.Execute(tuple);
                }
                ctx.WriteMsgQueueToFile("sentimentbolt.txt");
            }
            Console.WriteLine("Sentiment bolt done...");

            {
                LocalContext ctx = LocalContext.Get();
                GeographyBolt bolt = GeographyBolt.Get(ctx, emptyDictionary);

                ctx.ReadFromFileToMsgQueue("twitterspout.txt");
                List<SCPTuple> batch = ctx.RecvFromMsgQueue();
                foreach (SCPTuple tuple in batch)
                {
                    bolt.Execute(tuple);
                }
                ctx.WriteMsgQueueToFile("geographybolt.txt");
            }
            Console.WriteLine("Geography bolt done...");

            {
                LocalContext ctx = LocalContext.Get();
                MergeBolt bolt = MergeBolt.Get(ctx, emptyDictionary);

                List<SCPTuple> batch = new List<SCPTuple>();

                ctx.ReadFromFileToMsgQueue("sentimentbolt.txt");
                batch.AddRange(ctx.RecvFromMsgQueue());

                ctx.ReadFromFileToMsgQueue("geographybolt.txt");
                batch.AddRange(ctx.RecvFromMsgQueue());

                foreach (SCPTuple tuple in batch)
                {
                    bolt.Execute(tuple);
                }
                ctx.WriteMsgQueueToFile("mergebolt.txt");
            }
            Console.WriteLine("Merge bolt done...");

            {
                LocalContext ctx = LocalContext.Get();
                SwearWordBolt bolt = SwearWordBolt.Get(ctx, emptyDictionary);

                ctx.ReadFromFileToMsgQueue("mergebolt.txt");
                List<SCPTuple> batch = ctx.RecvFromMsgQueue();

                foreach (SCPTuple tuple in batch)
                {
                    bolt.Execute(tuple);
                }

                ctx.WriteMsgQueueToFile("swearwordbolt.txt");
            }
            Console.WriteLine("Swear bolt done...");

            //{
            //    LocalContext ctx = LocalContext.Get();
            //    HBaseTweetBolt bolt = HBaseTweetBolt.Get(ctx, emptyDictionary);

            //    ctx.ReadFromFileToMsgQueue("swearwordbolt.txt");
            //    List<SCPTuple> batch = ctx.RecvFromMsgQueue();

            //    foreach (SCPTuple tuple in batch)
            //    {
            //        bolt.Execute(tuple);
            //    }

            //    bolt.Execute(null);
            //}
            //Console.WriteLine("Tweet archive bolt done...");

            //{
            //    LocalContext ctx = LocalContext.Get();
            //    HBaseTopicBolt bolt = HBaseTopicBolt.Get(ctx, emptyDictionary);

            //    ctx.ReadFromFileToMsgQueue("swearwordbolt.txt");
            //    List<SCPTuple> batch = ctx.RecvFromMsgQueue();

            //    foreach (SCPTuple tuple in batch)
            //    {
            //        bolt.Execute(tuple);
            //    }

            //    bolt.Execute(null);
            //}
            //Console.WriteLine("Tweet topic bolt done...");
        }
    }
}
