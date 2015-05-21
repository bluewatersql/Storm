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
using Microsoft.WindowsAzure;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Auth;
using Microsoft.WindowsAzure.Storage.Blob;
using Newtonsoft.Json.Linq;

namespace TwitterStorm.Bolts
{
    public class BlobWriterBolt : ISCPBolt
    {
        #region Members
        private static string STORAGE_ACCOUNT_NAME;
        private static string STORAGE_ACCOUNT_KEY;
        private static string STORAGE_ACCOUNT_CONTAINER;
        private Context ctx;
        private Configuration cfg;
        private bool enableAck = false;

        private CloudBlobContainer blobContainer;
        #endregion

        public BlobWriterBolt(Context ctx, Dictionary<string, Object> parms)
        {
            Context.Logger.Info("Creating BlobWriterBolt");

            this.ctx = ctx;

            if (parms.ContainsKey("UserConfig"))
            {
                this.cfg = (Configuration)parms["UserConfig"];

                STORAGE_ACCOUNT_NAME = this.cfg.AppSettings.Settings["StorageAccountName"].Value;
                STORAGE_ACCOUNT_KEY = this.cfg.AppSettings.Settings["StorageAccountKey"].Value;
                STORAGE_ACCOUNT_CONTAINER = this.cfg.AppSettings.Settings["StorageAccountContainer"].Value;
            }

            StorageCredentials credentials = new StorageCredentials(STORAGE_ACCOUNT_NAME, 
                STORAGE_ACCOUNT_KEY);
            CloudStorageAccount storageAccount = new CloudStorageAccount(credentials, false);
            CloudBlobClient blobClient = storageAccount.CreateCloudBlobClient();
            this.blobContainer = blobClient.GetContainerReference(STORAGE_ACCOUNT_CONTAINER);            

            // Declare Input and Output schemas
            Dictionary<string, List<Type>> inputSchema = new Dictionary<string, List<Type>>();
            inputSchema.Add(Constants.DEFAULT_STREAM_ID, new List<Type>() { typeof(long), typeof(string) });

            //Dictionary<string, List<Type>> outputSchema = new Dictionary<string, List<Type>>();
            //outputSchema.Add(Constants.DEFAULT_STREAM_ID, new List<Type>() { typeof(long), typeof(string) });

            this.ctx.DeclareComponentSchema(new ComponentStreamSchema(inputSchema, null));

            //Check pluginConf info and enable ACK in Non-Tx topology
            if (Context.Config.pluginConf.ContainsKey(Constants.NONTRANSACTIONAL_ENABLE_ACK))
            {
                enableAck = (bool)(Context.Config.pluginConf[Constants.NONTRANSACTIONAL_ENABLE_ACK]);
            }
        }

        public void Execute(SCPTuple tuple)
        {
            Context.Logger.Info("BlobWriterBolt Execute");

            try
            {
                var id = tuple.GetLong(0);
                var tweet = tuple.GetString(1);

                //Write the blob
                var jObject = JObject.Parse(tweet);

                DateTime tweetDate = jObject.SelectToken("CreatedAt", true).Value<DateTime>();

                byte[] data = System.Text.Encoding.UTF8.GetBytes(tweet);
                string fileName = string.Format(@"Year={0}/Month={1}/Day={2}/{3}.json",
                        tweetDate.Year,
                        tweetDate.Month,
                        tweetDate.Day,
                        id);

                CloudBlockBlob blob = blobContainer.GetBlockBlobReference(fileName);
                
                using (MemoryStream ms = new MemoryStream(0))
                {
                    ms.Write(data, 0, data.Length);
                    ms.Seek(0, SeekOrigin.Begin);
                    blob.UploadFromStream(ms);
                }

                this.ctx.Emit(Constants.DEFAULT_STREAM_ID, new List<SCPTuple> { tuple }, new Values(id, tweet));

                Context.Logger.Info("BlobWriterBolt Uploaded file: {0}", fileName);

                if (enableAck)
                    this.ctx.Ack(tuple);
            }
            catch (Exception ex)
            {
                Context.Logger.Error("BlobWriterBolt Error: {0}", ex.Message);
                
                if (enableAck)
                    this.ctx.Fail(tuple);
            }

        }

        public static BlobWriterBolt Get(Context ctx, Dictionary<string, Object> parms)
        {
            return new BlobWriterBolt(ctx, parms);
        }
    }
}