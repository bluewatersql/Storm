using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;
using System.Threading;
using Microsoft.SCP;
using Microsoft.SCP.Rpc.Generated;
using System.Configuration;
using Newtonsoft.Json.Linq;
using System.Web;
using System.Net;

namespace TwitterStorm.Bolts
{
    public class SentimentBolt : ISCPBolt
    {
        #region Members
        private const string POSITIVE = "Positive";
        private const string NEGATIVE = "Negative";
        private const string NEUTRAL = "Neutral";
        private const string UNKNOWN = "Unknown";

        private static readonly string SENTIMENT_URL = @"http://www.sentiment140.com/api/classify?text={0}";

        private Context ctx;
        private bool enableAck = false;
        #endregion

        public SentimentBolt(Context ctx, Dictionary<string, Object> parms)
        {
            Context.Logger.Info("Creating SentimentBolt");

            this.ctx = ctx;

            // Declare Input and Output schemas
            Dictionary<string, List<Type>> inputSchema = new Dictionary<string, List<Type>>();
            inputSchema.Add(Constants.DEFAULT_STREAM_ID, new List<Type>() { typeof(long), typeof(string) });

            Dictionary<string, List<Type>> outputSchema = new Dictionary<string, List<Type>>();
            outputSchema.Add(TwitterStormConstants.SENTIMENT_STREAM, new List<Type>() { typeof(long), typeof(string), typeof(string) });

            this.ctx.DeclareComponentSchema(new ComponentStreamSchema(inputSchema, outputSchema));

            //Check pluginConf info and enable ACK in Non-Tx topology
            if (Context.Config.pluginConf.ContainsKey(Constants.NONTRANSACTIONAL_ENABLE_ACK))
            {
                enableAck = (bool)(Context.Config.pluginConf[Constants.NONTRANSACTIONAL_ENABLE_ACK]);
            }
        }

        public void Execute(SCPTuple tuple)
        {
            Context.Logger.Info("SentimentBolt Execute");

            try
            {
                var id = tuple.GetLong(0);
                var tweet = tuple.GetString(1);

                int polarity = -1; //Use -1 for a sentiment that can't be calculated

                //Parse the tweet JSON
                var jObject = JObject.Parse(tweet);
                var tweetText = jObject.SelectToken("Text").Value<string>();
                
                string url = string.Format(SENTIMENT_URL,
                    HttpUtility.UrlEncode(tweetText, System.Text.Encoding.UTF8));
                
                var request = HttpWebRequest.Create(url);
                using (var response = request.GetResponse())
                {
                    using (var streamReader = new StreamReader(response.GetResponseStream()))
                    {
                        try
                        {
                            // Read and parse source
                            var line = streamReader.ReadLine();
                            var resultObject = JObject.Parse(line);

                            polarity = resultObject.SelectToken("results", true).SelectToken("polarity", true).Value<int>();
                        }
                        catch (Exception ex)
                        {
                            Context.Logger.Error("SentimentBolt Sentiment Calculation Error: {0}", ex.Message);
                        }
                    }
                }

                string sentiment = UNKNOWN;

                switch (polarity)
                {
                    case (0):
                        sentiment = NEGATIVE;
                        break;
                    case (2):
                        sentiment = NEUTRAL;
                        break;
                    case (4):
                        sentiment = POSITIVE;
                        break;
                    default:
                        sentiment = UNKNOWN;
                        break;
                }
                this.ctx.Emit(TwitterStormConstants.SENTIMENT_STREAM, new List<SCPTuple> { tuple }, new Values(tuple.GetLong(0), tuple.GetString(1), sentiment));

                if (enableAck)
                    this.ctx.Ack(tuple);
            }
            catch (Exception ex)
            {
                Context.Logger.Error("SentimentBolt Error: {0}", ex.Message);

                if (enableAck)
                    this.ctx.Fail(tuple);
            }
        }

        public static SentimentBolt Get(Context ctx, Dictionary<string, Object> parms)
        {
            return new SentimentBolt(ctx, parms);
        }
    }
}