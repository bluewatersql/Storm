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
    public class GeographyBolt : ISCPBolt
    {
        #region Members
        private static readonly string BING_API_URL = @"http://dev.virtualearth.net/REST/v1/Locations/{0}?o=json&key={1}";
        private static string BING_API_KEY;
        
        private Context ctx;
        private Configuration cfg;
        private bool enableAck = false;

        #endregion

        public GeographyBolt(Context ctx, Dictionary<string, Object> parms)
        {
            Context.Logger.Info("Creating GeographyBolt");

            this.ctx = ctx;

            if (parms.ContainsKey("UserConfig"))
            {
                this.cfg = (Configuration)parms["UserConfig"];

                BING_API_KEY = this.cfg.AppSettings.Settings["BingApiKey"].Value;
            }

            // Declare Input and Output schemas
            Dictionary<string, List<Type>> inputSchema = new Dictionary<string, List<Type>>();
            inputSchema.Add(Constants.DEFAULT_STREAM_ID, new List<Type>() { typeof(long), typeof(string) });

            Dictionary<string, List<Type>> outputSchema = new Dictionary<string, List<Type>>();
            outputSchema.Add(TwitterStormConstants.GEOGRAPHY_STREAM, new List<Type>() { 
                typeof(long),   //id
                typeof(string), //Coordinates
                typeof(string), //AdminDistrict
                typeof(string), //AdminDistrict2
                typeof(string), //CountryRegion
                typeof(string), //Locality
                typeof(string)  //PostalCode
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
            Context.Logger.Info("GeographyBolt Execute");

            try
            {
                string formattedCoordinates = null;
                string adminDistrict = null;
                string adminDistrict2 = null;
                string countryRegion = null;
                string locality = null;
                string postalCode = null;

                var id = tuple.GetLong(0);
                var tweet = tuple.GetString(1);

                var jObject = JObject.Parse(tweet);
                var coordinates = jObject.SelectToken("Coordinates");

                if (coordinates != null)
                {
                    var latitude = coordinates.SelectToken("Latitude").Value<string>();
                    var longitude = coordinates.SelectToken("Longitude").Value<string>();
                    formattedCoordinates = string.Format("{0},{1}", latitude, longitude);

                    string url = string.Format(BING_API_URL,
                        formattedCoordinates,
                        BING_API_KEY);

                    var request = HttpWebRequest.Create(url);
                    using (var response = request.GetResponse())
                    {
                        using (var streamReader = new StreamReader(response.GetResponseStream()))
                        {
                            try
                            {
                                var line = streamReader.ReadLine();
                                var resultObject = JObject.Parse(line);

                                var resources= resultObject["resourceSets"][0]["resources"];

                                if (resources != null)
                                {
                                    var addr = resources[0]["address"];

                                    if (addr != null)
                                    {
                                        if (addr["adminDistrict"] != null)
                                            adminDistrict = addr["adminDistrict"].Value<string>();

                                        if (addr["adminDistrict2"] != null)
                                            adminDistrict2 = addr["adminDistrict2"].Value<string>();

                                        if (addr["countryRegion"] != null)
                                            countryRegion = addr["countryRegion"].Value<string>();

                                        if (addr["locality"] != null)
                                            locality = addr["locality"].Value<string>();

                                        if (addr["postalCode"] != null)
                                            postalCode = addr["postalCode"].Value<string>();
                                    }
                                }

                            }
                            catch (Exception ex)
                            {
                                Context.Logger.Error("SentimentBolt Sentiment Calculation Error: {0}", ex.Message);
                            }
                        }
                    }
                }

                this.ctx.Emit(TwitterStormConstants.GEOGRAPHY_STREAM, new List<SCPTuple> { tuple },
                    new Values(id, formattedCoordinates, adminDistrict, adminDistrict2, countryRegion, locality, postalCode));

                if (enableAck)
                    this.ctx.Ack(tuple);
            }
            catch (Exception ex)
            {
                Context.Logger.Error("GeographyBolt Error: {0}", ex.Message);

                if (enableAck)
                    this.ctx.Fail(tuple);
            }
        }

        public static GeographyBolt Get(Context ctx, Dictionary<string, Object> parms)
        {
            return new GeographyBolt(ctx, parms);
        }
    }
}