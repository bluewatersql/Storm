using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;
using System.Threading;
using Microsoft.SCP;
using Microsoft.SCP.Rpc.Generated;
using System.Configuration;
using Microsoft.ServiceBus;
using Microsoft.ServiceBus.Messaging;
using Microsoft.WindowsAzure;
using Newtonsoft.Json.Linq;

namespace TwitterStorm.Spouts
{
    public class TwitterSpout : ISCPSpout
    {
        #region Constants
        private static string QUEUE_CONNECTION_STRING;
        private static string QUEUE_NAME;
        private static int MAX_PENDING_TUPLE_NUM = 100;
        #endregion

        #region Members
        private Context ctx;
        private Configuration cfg;
        private QueueClient queueClient;

        private bool enableAck = false;
        private long lastSeqId = 0;
        private Dictionary<long, BrokeredMessage> cachedTuples = new Dictionary<long, BrokeredMessage>();
        #endregion

        public TwitterSpout(Context ctx, Dictionary<string, Object> parms = null)
        {
            Context.Logger.Info("Queued Twitter Spout Created");
            this.ctx = ctx;

            if (parms.ContainsKey("UserConfig"))
            {
                this.cfg = (Configuration)parms["UserConfig"];

                MAX_PENDING_TUPLE_NUM = Convert.ToInt32(this.cfg.AppSettings.Settings["TupleCache"].Value);
                QUEUE_CONNECTION_STRING = this.cfg.AppSettings.Settings["QueueConnectionString"].Value;
                QUEUE_NAME = this.cfg.AppSettings.Settings["QueueName"].Value;
            }

            // Declare Output schema
            Dictionary<string, List<Type>> outputSchema = new Dictionary<string, List<Type>>();
            outputSchema.Add("default", new List<Type>() { typeof(long), typeof(string) });

            this.ctx.DeclareComponentSchema(new ComponentStreamSchema(null, outputSchema));

            //Check pluginConf info and enable ACK in Non-Tx topology
            if (Context.Config.pluginConf.ContainsKey(Constants.NONTRANSACTIONAL_ENABLE_ACK))
            {
                enableAck = (bool)(Context.Config.pluginConf[Constants.NONTRANSACTIONAL_ENABLE_ACK]);
            }

            ReceiveMode recieveMode = ReceiveMode.ReceiveAndDelete;

            if (enableAck)
                recieveMode = ReceiveMode.PeekLock;

            this.queueClient = QueueClient.CreateFromConnectionString(QUEUE_CONNECTION_STRING, QUEUE_NAME, recieveMode);
        }
               
        public void NextTuple(Dictionary<string, Object> parms)
        {
            if (enableAck)
            {
                if (cachedTuples.Count <= MAX_PENDING_TUPLE_NUM)
                {
                    lastSeqId++;

                    try
                    {
                        var message = queueClient.Receive(new TimeSpan(0, 0, 5));

                        if (message != null)
                        {
                            string tweet = message.GetBody<string>();

                            var jObject = JObject.Parse(tweet);
                            var id = jObject.SelectToken("Id").Value<long>();
                            var language = jObject.SelectToken("Language").Value<string>();

                            if (!string.IsNullOrEmpty(language) && language.Equals("English"))
                            {
                                Context.Logger.Info("Emit msgId {0} for SeqId: {0}", message.MessageId, lastSeqId);
                                this.ctx.Emit(Constants.DEFAULT_STREAM_ID, new Values(id, tweet), lastSeqId);

                                cachedTuples[lastSeqId] = message;
                            }
                        }
                        else
                        {
                            Thread.Sleep(1000);
                        }
                    }
                    catch (Exception ex)
                    {
                        Context.Logger.Error("SwearWordBolt NextTuple Error: {0}", ex.Message);
                        Thread.Sleep(1000);
                    }
                }
                else
                {
                    Thread.Sleep(50);
                }
            }
            else
            {
                try
                {
                    var message = queueClient.Receive(new TimeSpan(0, 0, 5));

                    if (message != null)
                    {
                        string tweet = message.GetBody<string>();

                        var jObject = JObject.Parse(tweet);
                        var id = jObject.SelectToken("Id").Value<long>();
                        var language = jObject.SelectToken("Language").Value<string>();

                        if (!string.IsNullOrEmpty(language) && language.Equals("English"))
                        {
                            Context.Logger.Info("Emit msgId: {0}", message.MessageId);
                            this.ctx.Emit(new Values(id, tweet));
                        }
                    }
                }
                catch (Exception ex)
                {
                    Context.Logger.Error("SwearWordBolt NextTuple Error: {0}", ex.Message);
                    Thread.Sleep(1000);
                }
            }
        }

        public void Ack(long seqId, Dictionary<string, Object> parms)
        {
            Context.Logger.Info("Ack, seqId: {0}", seqId);

            if (cachedTuples.ContainsKey(seqId))
            {
                var message = cachedTuples[seqId];
                message.Complete();
                cachedTuples.Remove(seqId);
            }
            else
            {
                Context.Logger.Warn("Ack(), cached tuple for seqId {0} not found!", seqId);
            }
        }

        public void Fail(long seqId, Dictionary<string, Object> parms)
        {
            Context.Logger.Info("Fail, seqId: {0}", seqId);

            if (cachedTuples.ContainsKey(seqId))
            {
                var message = cachedTuples[seqId];
                message.Abandon();
                cachedTuples.Remove(seqId);
            }
            else
            {
                Context.Logger.Warn("Fail(), cached tuple for seqId {0} not found!", seqId);
            }
        }

        public static TwitterSpout Get(Context ctx, Dictionary<string, Object> parms)
        {
            return new TwitterSpout(ctx, parms);
        }
    }
}