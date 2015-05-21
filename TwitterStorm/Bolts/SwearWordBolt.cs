using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;
using System.Threading;
using Microsoft.SCP;
using Microsoft.SCP.Rpc.Generated;
using Newtonsoft.Json.Linq;
using System.Configuration;
using System.Text.RegularExpressions;
using TwitterStorm.Components;

namespace TwitterStorm.Bolts
{
    public class SwearWordBolt : ISCPBolt
    {
        #region Members
        private Context ctx;
        private bool enableAck = false;
        
        private List<string> dictionary;
        private List<string> noiseWords;

        private static readonly char[] PUNCTUATION_DELIMITERS = new[] {  
             ' ', '!', '\"', '#', '$', '%', '&', '\'', '(', ')', '*', '+', ',', '-', '.', '/',  
             ':', ';', '<', '=', '>', '?', '@', '[', ']', '^', '_', '`', '{', '|', '}', '~' }; 
        #endregion

        public SwearWordBolt(Context ctx, Dictionary<string, Object> parms)
        {
            Context.Logger.Info("Creating SwearWordBolt");

            this.ctx = ctx;

            // Declare Input and Output schemas
            Dictionary<string, List<Type>> inputSchema = new Dictionary<string, List<Type>>();
            inputSchema.Add(Constants.DEFAULT_STREAM_ID, new List<Type>() { 
                typeof(long), typeof(string), typeof(string), typeof(string), typeof(string),  
                typeof(string), typeof(string),  typeof(string), typeof(string) });

            Dictionary<string, List<Type>> outputSchema = new Dictionary<string, List<Type>>();
            outputSchema.Add(TwitterStormConstants.ARCHIVE_STREAM, new List<Type>() { 
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

            outputSchema.Add(TwitterStormConstants.TOPIC_STREAM, new List<Type>() { 
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

            this.ctx.DeclareComponentSchema(new ComponentStreamSchema(inputSchema, outputSchema));

            //Check pluginConf info and enable ACK in Non-Tx topology
            if (Context.Config.pluginConf.ContainsKey(Constants.NONTRANSACTIONAL_ENABLE_ACK))
            {
                enableAck = (bool)(Context.Config.pluginConf[Constants.NONTRANSACTIONAL_ENABLE_ACK]);
            }

            LoadDictionary();
            LoadNoiseWords();
        }

        public void Execute(SCPTuple tuple)
        {
            Context.Logger.Info("SwearWordBolt Execute");

            try
            {
                var id = tuple.GetLong(0);
                var tweet = tuple.GetString(2);
                
                var jObject = JObject.Parse(tweet);
                var body = jObject.SelectToken("Text").Value<string>();
                var createdDate = jObject.SelectToken("CreatedAt").Value<string>();

                //Strip emojis
                body = Regex.Replace(body, @"\p{Cs}", " ");

                //Strip Newlines
                body = body.Replace("\r\n", " ");

                //Strip single quotes
                body = body.Replace("'", string.Empty);

                //Strip URLs
                Regex urlRegex = new Regex(
                    @"(?:<\w+.*?>|[^=!:'""/]|^)((?:https?://|www\.)[-\w]+(?:\.[-\w]+)*(?::\d+)?(?:/(?:(?:[~\w\+%-]|(?:[,.;@:][^\s$]))+)?)*(?:\?[\w\+%&=.;:-]+)?(?:\#[\w\-\.]*)?)(?:\p{P}|\s|<|$)",
                    RegexOptions.IgnoreCase);

                foreach (var match in urlRegex.Matches(body))
                {
                    body = body.Replace(match.ToString(), " ");
                }

                //Strip user mentions
                Regex mentionRegex = new Regex(
                    @"@[A-Za-z0-9_-]*",
                    RegexOptions.IgnoreCase);

                foreach (var match in mentionRegex.Matches(body))
                {
                    body = body.Replace(match.ToString(), " ");
                }

                var words = body.ToLower().Split(PUNCTUATION_DELIMITERS);
                var topics = new List<string>();
                Regex numericRegex = new Regex(@"^\d+$");

                int swearWordCount = 0;
                

                foreach (var word in words)
                {
                    string cleanWord = word.Trim();

                    if (!string.IsNullOrEmpty(cleanWord) &&
                        !noiseWords.Contains(cleanWord) &&
                        !numericRegex.IsMatch(cleanWord))
                    {
                        //Do not emit swear words, just count them
                        if (dictionary.Contains(cleanWord))
                        {
                            swearWordCount++;
                        }

                        //Remove duplicates
                        if (!topics.Contains(cleanWord))
                            topics.Add(cleanWord);
                    }
                }

                this.ctx.Emit(TwitterStormConstants.ARCHIVE_STREAM, new List<SCPTuple> { tuple }, new Values(
                        tuple.GetLong(0),
                        createdDate,
                        tuple.GetString(1),
                        tuple.GetString(2),
                        tuple.GetString(3),
                        tuple.GetString(4),
                        tuple.GetString(5),
                        tuple.GetString(6),
                        tuple.GetString(7),
                        tuple.GetString(8),
                        swearWordCount
                        ));

                //Generate 2-gram from the body
                foreach (var topic in NGram.Generate(body, 2))
                {
                    var t = topic.ToLower().Trim();

                    if (!topics.Contains(t))
                        topics.Add(t);
                }

                foreach (var topic in topics)
                {
                    this.ctx.Emit(TwitterStormConstants.TOPIC_STREAM, new List<SCPTuple> { tuple }, new Values(
                        tuple.GetLong(0),
                        createdDate,
                        tuple.GetString(1),
                        topic,
                        tuple.GetString(3),
                        tuple.GetString(4),
                        tuple.GetString(5),
                        tuple.GetString(6),
                        tuple.GetString(7),
                        tuple.GetString(8),
                        (swearWordCount > 0)
                        ));
                }

                if (enableAck)
                    this.ctx.Ack(tuple);
            }
            catch (Exception ex)
            {
                Context.Logger.Error("SwearWordBolt Error: {0}", ex.Message);

                if (enableAck)
                    this.ctx.Fail(tuple);
            }
        }

        public static SwearWordBolt Get(Context ctx, Dictionary<string, Object> parms)
        {
            return new SwearWordBolt(ctx, parms);
        }

        private void LoadDictionary()
        {
            dictionary = File.ReadAllLines(@"data\dictionary.dat").ToList();
        }

        private void LoadNoiseWords()
        {
            noiseWords = File.ReadAllLines(@"data\noisewords.dat").ToList();
        }
    }
}