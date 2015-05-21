using Microsoft.SCP;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using TwitterStorm.Persistance;

namespace TwitterStorm.Bolts
{
    public class HBaseTweetBolt : ISCPBolt
    {
        #region Members
        private static string HBASE_SERVER_URL;
        private static string HBASE_USERNAME;
        private static string HBASE_PASSWORD;

        private Context ctx;
        private Configuration cfg;
        private bool enableAck = false;

        private QueuedHBaseWriter hbaseWriter;
        #endregion

        public HBaseTweetBolt(Context ctx, Dictionary<string, Object> parms)
        {
            Context.Logger.Info("Creating HBaseTweetBolt");

            this.ctx = ctx;

            if (parms.ContainsKey("UserConfig"))
            {
                this.cfg = (Configuration)parms["UserConfig"];

                HBASE_SERVER_URL = cfg.AppSettings.Settings["HBaseServerUrl"].Value;
                HBASE_USERNAME = cfg.AppSettings.Settings["HBaseUsername"].Value;
                HBASE_PASSWORD = cfg.AppSettings.Settings["HBasePassword"].Value;
            }

            this.hbaseWriter = new QueuedHBaseWriter(HBASE_SERVER_URL, HBASE_USERNAME, HBASE_PASSWORD);
            this.hbaseWriter.EnsureTweetTable();

            // Declare Input and Output schemas
            Dictionary<string, List<Type>> inputSchema = new Dictionary<string, List<Type>>();
            inputSchema.Add(TwitterStormConstants.ARCHIVE_STREAM, new List<Type>() {
                typeof(long),   //id
                typeof(string), //CreatedDate
                typeof(string), //Coordinates
                typeof(string), //TweetJSON
                typeof(string), //AdminDistrict
                typeof(string), //AdminDistrict2
                typeof(string), //CountryRegion
                typeof(string), //Locality
                typeof(string), //PostalCode
                typeof(string), //Sentiment
                typeof(int)     //SwearWordCount
            });

            inputSchema.Add(TwitterStormConstants.TOPIC_STREAM, new List<Type>() {
                typeof(long),   //id
                typeof(string), //CreatedDate
                typeof(string), //Coordinates
                typeof(string), //Topic
                typeof(string), //AdminDistrict
                typeof(string), //AdminDistrict2
                typeof(string), //CountryRegion
                typeof(string), //Locality
                typeof(string), //PostalCode
                typeof(string), //Sentiment
                typeof(bool)    //VulgarTweet
            });

            this.ctx.DeclareComponentSchema(new ComponentStreamSchema(inputSchema, null));

            if (!Context.Config.pluginConf.ContainsKey("topology.tick.tuple.freq.secs"))
                Context.Config.pluginConf.Add("topology.tick.tuple.freq.secs", 300);

            //Check pluginConf info and enable ACK in Non-Tx topology
            if (Context.Config.pluginConf.ContainsKey(Constants.NONTRANSACTIONAL_ENABLE_ACK))
            {
                enableAck = (bool)(Context.Config.pluginConf[Constants.NONTRANSACTIONAL_ENABLE_ACK]);
            }
        }

        public void Execute(SCPTuple tuple)
        {
            Context.Logger.Info("HBaseTweetBolt Execute");

            try
            {
                if (tuple.IsTickTuple() || tuple.GetSourceStreamId().Equals(TwitterStormConstants.ARCHIVE_STREAM))
                {
                    if (!tuple.IsTickTuple())
                    {
                        hbaseWriter.QueueTuple(tuple);

                        if (hbaseWriter.QueueCount > 500)
                        {
                            hbaseWriter.WriteTweetBatch(this.ctx, enableAck);
                        }
                    }
                    else
                    {
                        hbaseWriter.WriteTweetBatch(this.ctx, enableAck);
                    }
                }
            }
            catch (Exception ex)
            {
                Context.Logger.Error("HBaseTweetBolt Error: {0}", ex.Message);
            }
        }

        public static HBaseTweetBolt Get(Context ctx, Dictionary<string, Object> parms)
        {
            return new HBaseTweetBolt(ctx, parms);
        }
    }
}
