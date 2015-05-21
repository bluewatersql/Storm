using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;
using System.Threading;
using Microsoft.SCP;
using Microsoft.SCP.Rpc.Generated;
using TwitterStorm.Components;
using System.Configuration;

namespace TwitterStorm.Bolts
{
    public class SlidingWindowBolt : ISCPBolt
    {
        private Context ctx;
        private Configuration cfg;
        private bool enableAck = false;
        private static readonly int DEFAULT_NUM_WINDOW_CHUCKS = 5;
        private static readonly int DEFAULT_SLIDING_WINDOW_IN_SECONDS = DEFAULT_NUM_WINDOW_CHUCKS * 60;
        private static readonly int DEFAULT_RESULT_SIZE = 50;

        private SlidingWindowCounter<string> counter;
        private int windowLength;
        private int resultSize;

        public SlidingWindowBolt(Context ctx, Dictionary<string, Object> parms)
        {
            Context.Logger.Info("Creating SlidingWindowBolt");

            this.ctx = ctx;

            if (parms.ContainsKey("UserConfig"))
            {
                this.cfg = (Configuration)parms["UserConfig"];
            }

            // Declare Input and Output schemas
            Dictionary<string, List<Type>> inputSchema = new Dictionary<string, List<Type>>();
            inputSchema.Add(Constants.DEFAULT_STREAM_ID, new List<Type>() { typeof(string) });

            Dictionary<string, List<Type>> outputSchema = new Dictionary<string, List<Type>>();
            outputSchema.Add(Constants.DEFAULT_STREAM_ID, new List<Type>() { typeof(int), typeof(string), typeof(int) });

            this.ctx.DeclareComponentSchema(new ComponentStreamSchema(inputSchema, outputSchema));

            //Check pluginConf info and enable ACK in Non-Tx topology
            if (Context.Config.pluginConf.ContainsKey(Constants.NONTRANSACTIONAL_ENABLE_ACK))
            {
                enableAck = (bool)(Context.Config.pluginConf[Constants.NONTRANSACTIONAL_ENABLE_ACK]);
            }

            this.windowLength = DEFAULT_SLIDING_WINDOW_IN_SECONDS;
            this.resultSize = DEFAULT_RESULT_SIZE;
            this.counter = new SlidingWindowCounter<string>(windowLength);
            //Set tick tuple
        }

        public void Execute(SCPTuple tuple)
        {
            if (tuple.IsTickTuple())
            {
                var results = counter.GetWindowedCounts();

                var sortedResults = (from entry in results orderby entry.Value descending select entry)
                    .Take(resultSize)
                    .ToDictionary(pair => pair.Key, pair => pair.Value);

                int order = 1;

                foreach (var key in sortedResults.Keys)
                {
                    this.ctx.Emit(Constants.DEFAULT_STREAM_ID, new List<SCPTuple> { tuple }, new Values(order, key, results[key]));
                    order++;
                }
            }
            else
            {
                var hashtag = tuple.GetString(0);
                counter.Increment(hashtag);

                if (enableAck)
                    ctx.Ack(tuple);
            }
        }
    }
}