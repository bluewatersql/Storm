using Microsoft.HBase.Client;
using Microsoft.SCP;
using Newtonsoft.Json.Linq;
using org.apache.hadoop.hbase.rest.protobuf.generated;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace TwitterStorm.Persistance
{
    public class QueuedHBaseWriter
    {
        private static readonly string TWEET_TABLE = "tweets";
        private static readonly string TOPIC_TABLE = "topics";
        //private static readonly string HASHTAG_TABLE = "hashtags";

        private HBaseClient client = null;
        private Queue<SCPTuple> queue;

        public QueuedHBaseWriter(string url, string username, string password)
        {
            client = new HBaseClient(
                new ClusterCredentials(new Uri(url), username, password));

            queue = new Queue<SCPTuple>();
        }

        public int QueueCount { get { return queue.Count;  } }

        public void QueueTuple(SCPTuple tuple)
        {
            lock (queue)
            {
                queue.Enqueue(tuple);
            }
        }

        #region Write Tweet Batch
        public void WriteTweetBatch(Context ctx, bool enableAck)
        {
            List<SCPTuple> tuples = new List<SCPTuple>();
            int rows = 0;

            try
            {
                if (queue.Count > 0)
                {
                    lock (queue)
                    {
                        while (queue.Count > 0)
                            tuples.Add(queue.Dequeue());
                    }

                    var cellSet = new CellSet();

                    foreach (SCPTuple tuple in tuples)
                    {
                        var id = tuple.GetLong(0);
                        var createdDate = DateTime.Parse(tuple.GetString(1));
                        var coordinates = tuple.GetString(2);
                        var tweet = tuple.GetString(3);
                        var adminDistrict = tuple.GetString(4);
                        var adminDistrict2 = tuple.GetString(5);
                        var countryRegion = tuple.GetString(6);
                        var locality = tuple.GetString(7);
                        var postalCode = tuple.GetString(8);
                        var sentiment = tuple.GetString(9);
                        var swearWordCount = tuple.GetInteger(10);

                        var jObject = JObject.Parse(tweet);

                        var body = jObject.SelectToken("Text").Value<string>();

                        var isRetweet = jObject.SelectToken("IsRetweet", true).Value<string>();
                        var retweeted = jObject.SelectToken("Retweeted", true).Value<string>();
                        var retweetCount = jObject.SelectToken("RetweetCount", true).Value<string>();
                        var source = jObject.SelectToken("Source", true).Value<string>();

                        var profileID = jObject.SelectToken("Creator", true).SelectToken("Id", true).Value<string>();
                        var profileCreatedDate = jObject.SelectToken("Creator", true).SelectToken("CreatedAt", true).Value<string>();
                        var screenName = jObject.SelectToken("Creator", true).SelectToken("ScreenName", true).Value<string>();
                        var followersCount = jObject.SelectToken("Creator", true).SelectToken("FollowersCount", true).Value<string>();
                        var friendsCount = jObject.SelectToken("Creator", true).SelectToken("FriendsCount", true).Value<string>();
                        var statusesCount = jObject.SelectToken("Creator", true).SelectToken("StatusesCount", true).Value<string>();

                        var time_index = (ulong.MaxValue - (ulong)createdDate.ToBinary()).ToString().PadLeft(20) + id;
                        var key = screenName + "_" + time_index;

                        var row = new CellSet.Row { key = Encoding.UTF8.GetBytes(key) };

                        //Location Column Family
                        if (!string.IsNullOrEmpty(coordinates))
                        {
                            row.values.Add(CreateCell<string>("loc:coordinates", coordinates));

                            if (adminDistrict != null)
                                row.values.Add(CreateCell<string>("loc:admin_district1", adminDistrict));

                            if (adminDistrict2 != null)
                                row.values.Add(CreateCell<string>("loc:admin_district2", adminDistrict2));

                            if (countryRegion != null)
                                row.values.Add(CreateCell<string>("loc:country_region", countryRegion));

                            if (locality != null)
                                row.values.Add(CreateCell<string>("loc:locality", locality));

                            if (postalCode != null)
                                row.values.Add(CreateCell<string>("loc:postalcode", postalCode));
                        }

                        //User Column Family
                        row.values.Add(CreateCell<string>("user:profile_id", profileID));
                        row.values.Add(CreateCell<string>("user:screenname", screenName));
                        row.values.Add(CreateCell<string>("user:profile_created_dt", profileCreatedDate));
                        row.values.Add(CreateCell<string>("user:followers_count", followersCount));
                        row.values.Add(CreateCell<string>("user:friends_count", friendsCount));
                        row.values.Add(CreateCell<string>("user:statuses_count", statusesCount));

                        //Tweet Column Family
                        row.values.Add(CreateCell<long>("tweet:id", id));
                        row.values.Add(CreateCell<DateTime>("tweet:created_dt", createdDate));
                        row.values.Add(CreateCell<string>("tweet:text", body));
                        row.values.Add(CreateCell<string>("tweet:is_retweet", isRetweet));
                        row.values.Add(CreateCell<string>("tweet:retweeted", retweeted));
                        row.values.Add(CreateCell<string>("tweet:retweet_count", retweetCount));
                        row.values.Add(CreateCell<string>("tweet:source", source));
                        row.values.Add(CreateCell<string>("tweet:sentiment", sentiment));
                        row.values.Add(CreateCell<int>("tweet:swear_word_count", swearWordCount));

                        //Hashtag Column Family
                        int hashtagIndex = 1;

                        if (jObject.SelectToken("Hashtags") != null)
                        {
                            foreach (var hashtag in jObject.SelectToken("Hashtags"))
                            {
                                row.values.Add(CreateCell<string>(
                                    string.Format("ht:tag{0}", hashtagIndex),
                                    hashtag.Value<string>()));

                                hashtagIndex++;
                            }
                        }

                        cellSet.rows.Add(row);
                    }

                    client.StoreCells(TWEET_TABLE, cellSet);

                    if (enableAck)
                    {
                        foreach (var tuple in tuples)
                        {
                            ctx.Ack(tuple);
                        }
                    }

                    Context.Logger.Info("Messages written: {0}", rows);
                }
            }
            catch (Exception ex)
            {
                if (enableAck)
                {
                    foreach (var tuple in tuples)
                    {
                        ctx.Fail(tuple);
                    }
                }

                Context.Logger.Error("Exception: " + ex.Message);
            }
        }
        #endregion

        #region Write Topic Batch
        public void WriteTopicBatch(Context ctx, bool enableAck)
        {
            List<SCPTuple> tuples = new List<SCPTuple>();
            int rows = 0;

            try
            {
                if (queue.Count > 0)
                {
                    lock (queue)
                    {
                        while (queue.Count > 0)
                            tuples.Add(queue.Dequeue());
                    }

                    var cellSet = new CellSet();

                    foreach (SCPTuple tuple in tuples)
                    {
                        var id = tuple.GetLong(0);
                        var createdDate = DateTime.Parse(tuple.GetString(1));
                        var coordinates = tuple.GetString(2);
                        var topic = tuple.GetString(3);
                        var adminDistrict = tuple.GetString(4);
                        var adminDistrict2 = tuple.GetString(5);
                        var countryRegion = tuple.GetString(6);
                        var locality = tuple.GetString(7);
                        var postalCode = tuple.GetString(8);
                        var sentiment = tuple.GetString(9);
                        var vulgarTweet = tuple.GetBoolean(10);
                        

                        var time_index = (ulong.MaxValue - (ulong)createdDate.ToBinary()).ToString().PadLeft(20) + id.ToString();
                        var key = topic + "_" + time_index;

                        var row = new CellSet.Row { key = Encoding.UTF8.GetBytes(key) };

                        if (!string.IsNullOrEmpty(coordinates))
                        {
                            row.values.Add(CreateCell<string>("tp:coordinates", coordinates));

                            if (adminDistrict != null)
                                row.values.Add(CreateCell<string>("tp:admin_district1", adminDistrict));

                            if (adminDistrict2 != null)
                                row.values.Add(CreateCell<string>("tp:admin_district2", adminDistrict2));

                            if (countryRegion != null)
                                row.values.Add(CreateCell<string>("tp:country_region", countryRegion));

                            if (locality != null)
                                row.values.Add(CreateCell<string>("tp:locality", locality));

                            if (postalCode != null)
                                row.values.Add(CreateCell<string>("tp:postalcode", postalCode));
                        }

                        row.values.Add(CreateCell<DateTime>("tp:created_dt", createdDate));                        
                        row.values.Add(CreateCell<string>("tp:sentiment", sentiment));
                        row.values.Add(CreateCell<bool>("tp:vulgar_tweet", vulgarTweet));

                        cellSet.rows.Add(row);
                    }

                    client.StoreCells(TOPIC_TABLE, cellSet);

                    if (enableAck)
                    {
                        foreach (var tuple in tuples)
                        {
                            ctx.Ack(tuple);
                        }
                    }

                    Context.Logger.Info("Messages written: {0}", rows);
                }
            }
            catch (Exception ex)
            {
                if (enableAck)
                {
                    foreach (var tuple in tuples)
                    {
                        ctx.Fail(tuple);
                    }
                }

                Context.Logger.Error("Exception: " + ex.Message);
            }
        }
        #endregion

        #region Helpers
        private Cell CreateCell<T>(string columnNmae, T value)
        {
            return new Cell
            {
                column = Encoding.UTF8.GetBytes(columnNmae),
                data = Encoding.UTF8.GetBytes(value.ToString())
            };
        }
        #endregion

        #region Tables
        public void EnsureTweetTable()
        {
            if (!client.ListTables().name.Contains(TWEET_TABLE))
            {
                // Create the table
                var tableSchema = new TableSchema();
                tableSchema.name = TWEET_TABLE;
                tableSchema.columns.Add(new ColumnSchema { name = "tweet" });
                tableSchema.columns.Add(new ColumnSchema { name = "user" });
                tableSchema.columns.Add(new ColumnSchema { name = "loc" });
                tableSchema.columns.Add(new ColumnSchema { name = "ht" });
                client.CreateTable(tableSchema);
            }
        }

        public void EnsureTopicTable()
        {
            if (!client.ListTables().name.Contains(TOPIC_TABLE))
            {
                // Create the table
                var tableSchema = new TableSchema();
                tableSchema.name = TOPIC_TABLE;
                tableSchema.columns.Add(new ColumnSchema { name = "tp" });
                client.CreateTable(tableSchema);
            }
        }
        #endregion
    }
}
