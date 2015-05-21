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

namespace TwitterStorm.Bolts
{
    public class MergeBolt : ISCPBolt
    {
        #region Members
        private Context ctx;
        private Configuration cfg;
        private bool enableAck = false;

        private Dictionary<long, CachedTweet> tweetCache;
        #endregion

        public MergeBolt(Context ctx, Dictionary<string, Object> parms)
        {
            Context.Logger.Info("Creating MergeBolt");

            this.ctx = ctx;

            if (parms.ContainsKey("UserConfig"))
            {
                this.cfg = (Configuration)parms["UserConfig"];
            }

            tweetCache = new Dictionary<long, CachedTweet>();

            // Declare Input and Output schemas
            Dictionary<string, List<Type>> inputSchema = new Dictionary<string, List<Type>>();
            inputSchema.Add(TwitterStormConstants.SENTIMENT_STREAM, new List<Type>() { typeof(long), typeof(string), typeof(string) });
            inputSchema.Add(TwitterStormConstants.GEOGRAPHY_STREAM, new List<Type>() { typeof(long), typeof(string), typeof(string), typeof(string), typeof(string), typeof(string), typeof(string) });
            
            Dictionary<string, List<Type>> outputSchema = new Dictionary<string, List<Type>>();
            outputSchema.Add(Constants.DEFAULT_STREAM_ID, new List<Type>() { 
                typeof(long),   //id
                typeof(string), //Coordinates
                typeof(string), //TweetJSON
                typeof(string), //AdminDistrict
                typeof(string), //AdminDistrict2
                typeof(string), //CountryRegion
                typeof(string), //Locality
                typeof(string), //PostalCode
                typeof(string)  //Sentiment
            });

            this.ctx.DeclareComponentSchema(new ComponentStreamSchema(inputSchema, outputSchema));

            //Check pluginConf info and enable ACK in Non-Tx topology
            if (Context.Config.pluginConf.ContainsKey(Constants.NONTRANSACTIONAL_ENABLE_ACK))
            {
                enableAck = (bool)(Context.Config.pluginConf[Constants.NONTRANSACTIONAL_ENABLE_ACK]);
            }
        }

        public void Execute(SCPTuple tuple)
        {
            Context.Logger.Info("MergeBolt Execute");

            var id = tuple.GetLong(0);
            CachedTweet cachedTweet = null;

            if (tweetCache.ContainsKey(id))
            {
                cachedTweet = tweetCache[id];
            }
            else
            {
                cachedTweet = new CachedTweet(id);
                tweetCache.Add(id, cachedTweet);                
            }

            try
            {
                if (tuple.GetSourceStreamId().Equals("sentiment"))
                {
                    cachedTweet.SetSentiment(tuple.GetString(1), tuple.GetString(2));
                }
                else if (tuple.GetSourceStreamId().Equals("geography"))
                {
                    cachedTweet.SetGeography(
                            tuple.GetString(1),
                            tuple.GetString(2),
                            tuple.GetString(3),
                            tuple.GetString(4),
                            tuple.GetString(5),
                            tuple.GetString(6)
                        );
                }

                //Cache the tuple so we can ACK it once the join/merge is complete
                if (enableAck)
                {
                    cachedTweet.CachedTuples.Add(tuple);
                }

                //Determine if we've lined everything up
                if (cachedTweet.IsComplete())
                {
                    this.ctx.Emit(Constants.DEFAULT_STREAM_ID, new List<SCPTuple> { tuple }, 
                        new Values(id, cachedTweet.Coordinates, cachedTweet.TweetJson,
                            cachedTweet.AdminDistrict, cachedTweet.AdminDistrict2, cachedTweet.CountryRegion,
                            cachedTweet.Locality, cachedTweet.PostalCode, cachedTweet.Sentiment));

                    if (enableAck)
                    {
                        foreach (var t in cachedTweet.CachedTuples)
                        {
                            this.ctx.Ack(t);
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                Context.Logger.Error("MergeBolt Error: {0}", ex.Message);

                if (enableAck && cachedTweet != null)
                {
                    foreach (var t in cachedTweet.CachedTuples)
                    {
                        this.ctx.Fail(t);
                    }
                }
            }
        }

        public static MergeBolt Get(Context ctx, Dictionary<string, Object> parms)
        {
            return new MergeBolt(ctx, parms);
        }

        #region Cached Tweet
        internal class CachedTweet
        {
            #region Members
            private bool hasSentiment;
            private bool hasGeography;
            #endregion

            #region Constructor
            public CachedTweet(long id)
            {
                this.Id = id;
                this.CachedTuples = new List<SCPTuple>();
            }
            #endregion

            #region Properties
            public long Id { get; private set; }
            public string TweetJson { get; private set; }
            public string Coordinates { get; private set; }
            public string AdminDistrict { get; private set; }
            public string AdminDistrict2 { get; private set; }
            public string CountryRegion { get; private set; }
            public string Locality { get; private set; }
            public string PostalCode { get; private set; }
            public string Sentiment { get; private set; }

            public List<SCPTuple> CachedTuples { get; set; }
            #endregion

            #region Methods
            public void SetSentiment(string json, string mood) 
            { 
                hasSentiment = true;

                this.TweetJson = json;
                this.Sentiment = mood;
            }
            public void SetGeography(string coords, string adminDistrict, string adminDistrict2, string countryRegion, string locality, string postalCode) 
            { 
                hasGeography = true;

                this.Coordinates = coords;
                this.AdminDistrict = adminDistrict;
                this.AdminDistrict2 = adminDistrict2;
                this.CountryRegion = countryRegion;
                this.Locality = locality;
                this.PostalCode = postalCode;
            }

            public bool IsComplete()
            {
                return (hasSentiment && hasGeography);
            }
            #endregion
        }
        #endregion
    }
}